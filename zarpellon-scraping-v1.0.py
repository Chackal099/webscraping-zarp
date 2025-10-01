# -*- coding: utf-8 -*-
from __future__ import annotations
import os, re, time, json, logging, sys, threading, random
from dataclasses import dataclass, asdict, field
from typing import List, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse
from queue import Queue, Empty
from pathlib import Path
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FFOptions
from selenium.webdriver.firefox.service import Service as FFService
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, JavascriptException
from webdriver_manager.firefox import GeckoDriverManager

# ============================== Config & Logging ==============================
env_path = Path(__file__).with_name("logininfo.env")
if env_path.exists():
    load_dotenv(env_path, override=False)
else:
    load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("scraper.log", mode="a", encoding="utf-8")],
)
log = logging.getLogger("zarpellon")

# ------------------------------ Ajustes gerais ------------------------------- 
BASE       = "https://zarpellonjoias.com.br"                                    # URL raiz do site alvo.
LOGIN_PATH = "/login"                                                           # Caminho relativo da página de login.
OUT_JSON   = "produtos_scrape.json"                                            # Nome do arquivo JSON de saída gerado pelo scraping.
EMAIL  = os.getenv("ZARPELLON_USER")                                           # Usuário (e-mail) lido do .env (variável ZARPELLON_USER).
PWD    = os.getenv("ZARPELLON_PASS")                                           # Senha lida do .env (variável ZARPELLON_PASS).

# Concurrency e tempos (scraping)
N_WORKERS               = 4                                                     # Número de workers (threads) para raspar páginas/produtos em paralelo.
PAGELOAD_TIMEOUT_S      = 15                                                    # Tempo máximo (segundos) para esperar o carregamento de uma página.
AFTER_NAV_DELAY_S       = 0.25                                                  # Pausa curta após cada navegação (reduz race conditions).
PRODUCT_READY_TIMEOUT_S = 1.5                                                   # Janela (segundos) para aguardar elementos essenciais do produto aparecerem.
BETWEEN_PAGES_DELAY_S   = 0.20                                                  # Atraso entre trocas de página/paginação para não “martelar” o servidor.
SCROLL_JIGGLE           = True                                                  # Se True, faz pequenos scrolls para forçar lazy-load de elementos.
BLOCK_IMAGES            = False                                                 # Se True, bloqueia imagens (economiza banda; pode quebrar alguns seletores).
REFERER_HOP_ON_RETRY    = True                                                  # Se True, ajusta/enche o header Referer nas tentativas (melhora aceitação).

# Retry/backoff itens
RETRY_MAX_TRIES     = 6                                                         # Número máximo de tentativas por recurso (com backoff).
RETRY_BACKOFF_BASE  = 1.5                                                       # Base do backoff exponencial entre tentativas (1.5, 2.0, etc.).
QUIET_AFTER_403_S   = 5.0                                                       # “Silêncio” extra (segundos) após um HTTP 403, para evitar bloqueios.

# Paginação
MAX_PAGES_PER_CAT   = 2000                                                      # Teto de páginas por categoria (anti-loop/anti-paginação infinita).
ENABLE_SLOW_RETRY   = True                                                      # Ativa rota de retry mais “lenta” (interv. maiores) se falhas persistirem.

# Categorias (raiz do site)
CATEGORIES = {
    "Anéis":      f"{BASE}/categorias-aneis",                                   # URL da lista de produtos da categoria Anéis.
    "Berloques":  f"{BASE}/categorias-berloques",                               # URL da lista de produtos da categoria Berloques.
    "Brincos":    f"{BASE}/categorias-brincos",                                 # URL da lista de produtos da categoria Brincos.
    "Colares":    f"{BASE}/categorias-colares",                                 # URL da lista de produtos da categoria Colares.
    "Conjuntos":  f"{BASE}/categorias-conjuntos",                               # URL da lista de produtos da categoria Conjuntos.
    "Pingentes":  f"{BASE}/categorias-pingentes",                               # URL da lista de produtos da categoria Pingentes.
    "Pulseiras":  f"{BASE}/categorias-pulseiras",                               # URL da lista de produtos da categoria Pulseiras.
}

# UA/idioma
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:141.0) Gecko/20100101 Firefox/141.0"  # User-Agent “desktop Firefox” a ser enviado.
ACCEPT_LANG = "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"                                      # Preferência de idioma nas requisições HTTP.

# ============================== Utils ==============================
def normalize_url(u: str) -> str:
    if not u: return u
    p = urlparse(u); return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

def unique(seq: List[str]) -> List[str]:
    seen=set(); out=[]
    for x in seq:
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out

def _clean(txt: str) -> str:
    return re.sub(r"\s+", " ", (txt or "").replace("\xa0"," ")).strip()

def product_base_id(url: str) -> Optional[str]:
    m = re.search(r"/produto/(\d+)(?::\d+)?/", url)
    return m.group(1) if m else None

# ============================== Firefox setup ==============================
def build_firefox_options(headless=True) -> FFOptions:
    opts = FFOptions()
    if headless:
        opts.add_argument("--headless")
    opts.set_preference("general.useragent.override", UA)
    opts.set_preference("intl.accept_languages", ACCEPT_LANG)
    opts.set_preference("dom.webdriver.enabled", False)
    opts.set_preference("network.http.sendRefererHeader", 2)
    opts.set_preference("network.dns.disablePrefetch", True)
    opts.set_preference("network.prefetch-next", False)
    opts.set_preference("network.predictor.enabled", False)
    if BLOCK_IMAGES:
        opts.set_preference("permissions.default.image", 2)
    opts.page_load_strategy = "eager"
    return opts

def _post_warmup_stealth(driver):
    try:
        driver.get("about:blank")
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    except Exception: pass

def new_driver(gecko_path: str, headless=True):
    drv = webdriver.Firefox(service=FFService(gecko_path), options=build_firefox_options(headless=headless))
    drv.set_page_load_timeout(PAGELOAD_TIMEOUT_S)
    _post_warmup_stealth(drv)
    try:
        import random as _r
        drv.set_window_size(_r.randint(1200, 1440), _r.randint(780, 920))
    except Exception: pass
    return drv

# ============================== Login no site (para scraping) ==============================
def looks_logged_html(html: str) -> bool:
    t=(html or "").lower()
    return ("meus pedidos" in t) or ("sair" in t) or ("meus dados" in t)

def safe_page_source(driver) -> str:
    for _ in range(3):
        try: return driver.page_source
        except Exception: time.sleep(0.4)
    return ""

def accept_cookies(wait: WebDriverWait):
    locs = [
        (By.XPATH, "//button[contains(translate(.,'ACEITAR','aceitar'),'aceitar')]"),
        (By.XPATH, "//button[contains(translate(.,'OK, ENTENDI','ok, entendi'),'ok, entendi')]"),
        (By.XPATH, "//*[contains(., 'Aceitar') or contains(., 'Ok, entendi')]"),
    ]
    for by, sel in locs:
        try:
            wait.until(EC.element_to_be_clickable((by, sel))).click(); break
        except Exception: pass

def login_and_collect_auth(gecko_path: str, headless=True) -> Tuple[webdriver.Firefox, List[dict], Dict[str,str]]:
    if not EMAIL or not PWD:
        raise RuntimeError("Credenciais ausentes. Defina ZARPELLON_USER e ZARPELLON_PASS no .env")

    driver = new_driver(gecko_path, headless=headless)
    wait = WebDriverWait(driver, 25)
    log.info("Abrindo %s", urljoin(BASE, LOGIN_PATH))
    driver.get(urljoin(BASE, LOGIN_PATH))
    accept_cookies(wait)

    email_el = wait.until(EC.presence_of_element_located((By.NAME, "email")))
    senha_el = wait.until(EC.presence_of_element_located((By.NAME, "senha")))
    email_el.clear(); email_el.send_keys(EMAIL)
    senha_el.clear(); senha_el.send_keys(PWD)

    try:
        btn = wait.until(EC.element_to_be_clickable((By.ID, "btn_enviar_cadastro")))
    except TimeoutException:
        btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']")))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'}); arguments[0].click();", btn)

    wait = WebDriverWait(driver, 40)
    try: wait.until(lambda d: looks_logged_html(safe_page_source(d)))
    except TimeoutException: raise RuntimeError("Login não confirmado.")

    cookies = driver.get_cookies()
    try: localstorage = driver.execute_script("return Object.assign({}, window.localStorage);") or {}
    except Exception: localstorage = {}

    log.info("Login confirmado.")
    return driver, cookies, localstorage

def prime_auth_on_driver(driver, cookies: List[dict], localstorage: Dict[str,str]):
    driver.get(BASE + "/")
    for c in cookies:
        try:
            c2 = {k: c[k] for k in ("name","value","path","domain","expiry","httpOnly","secure") if k in c}
            driver.add_cookie(c2)
        except Exception: pass
    driver.refresh()
    try:
        driver.execute_script("""
            const data = arguments[0];
            for (const k in data) { try { localStorage.setItem(k, data[k]); } catch(e){} }
        """, localstorage)
    except Exception: pass

# ============================== Coleta de links (paginada) ==============================
JS_GRAB_LINKS = """
return Array.from(document.querySelectorAll("a[href*='/produto/'], a[href^='/p/'], a[href^='/produto/']"))
  .map(a => a.href).filter(Boolean);
"""
JS_PAGE_SIG = r"""
return (()=>{
  const as = Array.from(document.querySelectorAll("a[href*='/produto/'], a[href^='/p/'], a[href^='/produto/']"))
                  .map(a => a.href).filter(Boolean);
  if (!as.length) return "";
  const head = as.slice(0, 3).join("|");
  const tail = as.slice(-3).join("|");
  return head + "::" + tail + "::" + as.length;
})();
"""

def page_signature(driver) -> str:
    try: sig = driver.execute_script(JS_PAGE_SIG) or ""
    except Exception: sig = ""
    return str(sig)

def js_collect_links(driver) -> list[str]:
    try: hrefs = driver.execute_script(JS_GRAB_LINKS) or []
    except Exception: hrefs = []
    out=set()
    for h in hrefs:
        full = normalize_url((h or "").split("#",1)[0])
        if re.search(r"/produto[s]?/|/p/", full): out.add(full)
    return sorted(out)

JS_HREFS_IN_SCRIPTS = r"""
return Array.from(document.querySelectorAll('script')).flatMap(s => {
  const t = s.textContent || '';
  const out = [];
  const re = /"href":"(\/produto\/[^"\\]+)"/g;
  let m;
  while ((m = re.exec(t)) !== null) {
    try { out.push(new URL(m[1], location.origin).href); } catch(e) {}
  }
  return out;
});
"""

def js_collect_links_from_scripts(driver) -> list[str]:
    try: hrefs = driver.execute_script(JS_HREFS_IN_SCRIPTS) or []
    except Exception: hrefs = []
    out=set()
    for h in hrefs:
        if not h: continue
        h = normalize_url(h.split("#,")[0] if "#," in h else h.split("#", 1)[0])
        if re.search(r"/produto[s]?/|/p/", h): out.add(h)
    return sorted(out)

def wait_grid_ready(driver, timeout=6) -> None:
    WebDriverWait(driver, timeout, poll_frequency=0.2).until(
        EC.presence_of_element_located((By.CSS_SELECTOR,
            "a[href*='/produto/'], a[href^='/p/'], a[href^='/produto/']"))
    )

def _ensure_paginator_visible(driver) -> None:
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 200);")
        time.sleep(0.18)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception: pass
    time.sleep(0.18)

def _page_numbers_from_dom(driver) -> list[int]:
    nums=[]
    try:
        btns = driver.find_elements(By.CSS_SELECTOR, ".paginacao-lista .paginas *")
        for b in btns:
            t=(b.text or "").strip()
            if t.isdigit(): nums.append(int(t))
    except Exception: pass
    seen=set(); out=[]
    for n in nums:
        if n not in seen: seen.add(n); out.append(n)
    return out

def _click_page_number_fast(driver, page_num: int, timeout: int = 6) -> bool:
    sig_before = page_signature(driver)
    xps = [
        f"//div[contains(@class,'paginacao-lista')]//div[contains(@class,'paginas')]//*[normalize-space(text())='{page_num}']",
        f"//nav[contains(@aria-label,'agina') or contains(@aria-label,'Page')]//*[normalize-space(text())='{page_num}']",
    ]
    for xp in xps:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                if not el.is_displayed(): continue
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'}); arguments[0].click();", el)
                except Exception:
                    try: el.click()
                    except Exception: continue
                try:
                    WebDriverWait(driver, timeout).until(lambda d: page_signature(d) != sig_before)
                except TimeoutException:
                    time.sleep(BETWEEN_PAGES_DELAY_S)
                    if page_signature(driver) == sig_before: continue
                time.sleep(BETWEEN_PAGES_DELAY_S)
                return True
        except Exception:
            continue
    return False

def _click_next(driver, timeout: int = 6) -> bool:
    sig_before = page_signature(driver)
    XPS = [
        "//div[contains(@class,'paginacao-lista')]//*[self::a or self::button][contains(@class,'proximo') or contains(@class,'next')][not(contains(@class,'disabled')) and not(@disabled)]",
        "//*[self::a or self::button][contains(.,'Próximo') or contains(.,'Proximo') or normalize-space(.)='›' or normalize-space(.)='>'][not(contains(@class,'disabled')) and not(@disabled)]"
    ]
    for xp in XPS:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                if not el.is_displayed(): continue
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'}); arguments[0].click();", el)
                except Exception:
                    try: el.click()
                    except Exception: continue
                try:
                    WebDriverWait(driver, timeout).until(lambda d: page_signature(d) != sig_before)
                except TimeoutException:
                    time.sleep(BETWEEN_PAGES_DELAY_S)
                    if page_signature(driver) == sig_before: continue
                time.sleep(BETWEEN_PAGES_DELAY_S)
                return True
        except Exception: pass
    return False

def _click_load_more(driver, timeout: int = 6) -> bool:
    LOAD_MORE_XPATHS = [
        "//button[contains(translate(.,'CARREGAR','carregar'),'carregar')]",
        "//button[contains(translate(.,'MOSTRAR MAIS','mostrar mais'),'mostrar mais')]",
        "//button[contains(.,'Ver mais') or contains(.,'VER MAIS')]",
        "//a[contains(@class,'carregar') or contains(@class,'mais')][not(contains(@class,'disabled'))]"
    ]
    for xp in LOAD_MORE_XPATHS:
        try:
            els = driver.find_elements(By.XPATH, xp)
            for el in els:
                if el.is_displayed() and el.is_enabled():
                    try:
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'}); arguments[0].click();", el)
                    except Exception:
                        try: el.click()
                        except Exception: continue
                    time.sleep(BETWEEN_PAGES_DELAY_S)
                    return True
        except Exception: pass
    return False

def collect_all_links_with_pagination(driver, cat_url: str) -> list[str]:
    try: driver.get(BASE + "/"); time.sleep(0.12)
    except Exception: pass

    driver.get(cat_url)
    try: wait_grid_ready(driver, timeout=10)
    except Exception: time.sleep(0.25)

    _ensure_paginator_visible(driver)

    all_links: set[str] = set(js_collect_links(driver))
    nums = _page_numbers_from_dom(driver)

    if nums:
        last = max(nums)
        for page in range(2, min(last, MAX_PAGES_PER_CAT) + 1):
            if not _click_page_number_fast(driver, page, timeout=6):
                if not _click_next(driver, timeout=6):
                    break
            try: wait_grid_ready(driver, timeout=4)
            except Exception: pass
            time.sleep(0.10)
            all_links.update(js_collect_links(driver))
        return sorted(all_links)

    steps = 0
    while steps < MAX_PAGES_PER_CAT:
        steps += 1
        prev = len(all_links)
        if not _click_next(driver, timeout=6): break
        try: wait_grid_ready(driver, timeout=4)
        except Exception: pass
        time.sleep(0.10)
        all_links.update(js_collect_links(driver))
        if len(all_links) <= prev: break

    rounds = 0; last_cnt = len(all_links)
    while rounds < 12:
        rounds += 1
        before = len(all_links)
        if not _click_load_more(driver, timeout=6):
            try: driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            except Exception: pass
            time.sleep(0.20)
        try: wait_grid_ready(driver, timeout=4)
        except Exception: pass
        time.sleep(0.10)
        all_links.update(js_collect_links(driver))
        if len(all_links) <= before or len(all_links) == last_cnt: break

    return sorted(all_links)

def discover_subcategory_urls(driver, cat_url: str) -> list[str]:
    path = urlparse(cat_url).path.strip("/").lower()
    m = re.match(r"(categorias-[a-z0-9-]+)", path)
    if not m: return []
    base_seg = m.group(1); prefix = "/" + base_seg + "-"

    try:
        hrefs = driver.execute_script("""
            const pref = arguments[0];
            return Array.from(document.querySelectorAll('a[href]'))
              .map(a => a.href)
              .filter(h => h.includes(pref));
        """, prefix) or []
    except Exception: hrefs = []

    urls = []
    for h in hrefs:
        if not h: continue
        u = normalize_url(h)
        if u.startswith(BASE) and prefix in u: urls.append(u)

    seen=set(); out=[]
    for u in urls:
        if u not in seen:
            seen.add(u); out.append(u)
    return out

def collect_links_category_and_subs(driver, cat_url: str) -> list[str]:
    all_links: set[str] = set()

    links_cat = collect_all_links_with_pagination(driver, cat_url)
    all_links.update(links_cat)
    if len(links_cat) < 80:
        try: driver.get(cat_url); time.sleep(0.25)
        except Exception: pass
        all_links.update(js_collect_links_from_scripts(driver))

    subcats = discover_subcategory_urls(driver, cat_url)
    subcats = [u for u in subcats if normalize_url(u) != normalize_url(cat_url)]
    for sub in subcats:
        links_sub = collect_all_links_with_pagination(driver, sub)
        all_links.update(links_sub)
        if len(links_sub) < 60:
            try: driver.get(sub); time.sleep(0.2)
            except Exception: pass
            all_links.update(js_collect_links_from_scripts(driver))

    return sorted(all_links)
# ============================== Modelo & Parsing ==============================
@dataclass
class ProductItem:
    url: str
    title: Optional[str] = None
    sku_base: Optional[str] = None
    description: Optional[str] = None
    images: List[str] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    variations: List[Dict] = field(default_factory=list)
    children: List[Dict] = field(default_factory=list)
    materials: List[str] = field(default_factory=list)
    price: Optional[float] = None

def wait_for_product_ready(driver, timeout=PRODUCT_READY_TIMEOUT_S):
    wait = WebDriverWait(driver, timeout, poll_frequency=0.2)
    try: wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".componente-produto-detalhes")))
    except TimeoutException: return
    try:
        if SCROLL_JIGGLE:
            driver.execute_script("window.scrollTo(0, 160);")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight*0.6);")
            driver.execute_script("window.scrollTo(0, 0);")
    except JavascriptException: pass

def parse_title_desc_imgs(html: str, url: str, cat_label: Optional[str]) -> ProductItem:
    soup = BeautifulSoup(html, "lxml")
    area = soup.select_one(".componente-produto-detalhes") or soup

    def gx(sel):
        el = area.select_one(sel) or soup.select_one(sel)
        return _clean(el.get_text(" ", strip=True)) if el else None

    title = gx(".componente-detalhes-infos .descricao-curta") or gx("h1")
    description = gx(".descricao-produto")

    images=[]
    for img in area.select(".componente-imagens-grid img[src]"): images.append(img["src"])
    for fig in area.select(".componente-imagens-grid figure[style*='background-image']"):
        style = fig.get("style") or ""
        m = re.search(r'url\(["\']?(https?://[^)"\']+)', style)
        if m: images.append(m.group(1))
    images = unique([u for u in images if "web.solvis.net.br/smileys" not in u])

    categories=[cat_label] if cat_label else []

    materials=[]
    for li in area.select(".descricao-produto li"):
        t=_clean(li.get_text(" ", strip=True))
        if re.search(r"\b(Aço|Prata|Ródio|Rhodium|Ouro|Folheado|Banho)\b", t, re.I):
            materials.append(t)
    materials = unique(materials)[:10]

    return ProductItem(url=url, title=title, description=description, images=images, categories=categories, materials=materials)

# ============================== Variações (coleta simplificada) ==============================
def _norm_label(lbl: str) -> str:
    t = re.sub(r"\s+", " ", (lbl or "").strip())
    tl = t.lower()
    if tl.startswith("banh"):   return "Material"
    if "cor" in tl:             return "Cor"
    if tl.startswith("taman") or tl in {"numeração","numeracao","aro"}: return "Tamanho"
    return t or "Opção"

def _read_sku_and_stock(driver) -> Tuple[Optional[str], Optional[int]]:
    sku_txt=None
    try:
        ref_el = driver.find_element(By.CSS_SELECTOR, ".componente-detalhes-infos .componente-referencia .referencia, .desc-curta-e-ref")
        sku_txt = _clean(ref_el.text)
        m = re.search(r"([0-9A-Za-z._/-]+)\s*$", sku_txt or "")
        if m: sku_txt = m.group(1)
    except Exception: pass
    stock=None
    try:
        est_el = driver.find_element(By.CSS_SELECTOR, ".componente-detalhes-infos .componente-estoque .estoque")
        m = re.search(r"(\d+)", _clean(est_el.text).replace(".",""))
        if m: stock = int(m.group(1))
    except Exception: pass
    return sku_txt, stock

def _find_variation_blocks(driver):
    return driver.find_elements(By.CSS_SELECTOR, ".componente-detalhes-variacoes .variacao-tipo")

def _list_group_options_text(driver, block) -> Tuple[str,List[str],Dict]:
    try: raw_label = block.find_element(By.CSS_SELECTOR, ".tipo").text
    except Exception: raw_label = "Opção"
    label = _norm_label(raw_label)
    opts=[]; meta={"type":None,"el":None}
    try:
        chips = block.find_elements(By.CSS_SELECTOR, ".variacoes .variacao")
        for el in chips:
            txt = re.sub(r"\s+", " ", (el.text or "").strip())
            if not txt: continue
            if label=="Tamanho":
                m = re.search(r"(\d{1,2})", txt)
                if m: txt = m.group(1)
            opts.append(txt)
        if opts: meta={"type":"chips","el":block}
    except Exception: pass
    try:
        for sel in block.find_elements(By.TAG_NAME, "select"):
            s = Select(sel)
            for opt in s.options:
                t = re.sub(r"\s+", " ", (opt.text or "").strip())
                if t and t.lower() not in {"selecione","selecionar","escolha uma opção","choose an option"}:
                    if label=="Tamanho":
                        m = re.search(r"(\d{1,2})", t)
                        if m: t = m.group(1)
                    if t not in opts: opts.append(t)
            if opts and meta["type"] is None: meta={"type":"select","el":sel}
    except Exception: pass
    return label, opts, meta

def _select_option(driver, label: str, meta: Dict, value: str) -> bool:
    try:
        if meta["type"]=="chips":
            candidates = meta["el"].find_elements(By.CSS_SELECTOR, ".variacoes .variacao")
            for el in candidates:
                txt = re.sub(r"\s+", " ", (el.text or "").strip())
                if label=="Tamanho":
                    m = re.search(r"(\d{1,2})", txt)
                    if m: txt = m.group(1)
                if txt.strip().lower() == value.strip().lower():
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    try: driver.execute_script("arguments[0].click();", el)
                    except Exception: el.click()
                    break
            time.sleep(0.2); return True
        elif meta["type"]=="select":
            sel = Select(meta["el"])
            try: sel.select_by_visible_text(value)
            except Exception:
                if label=="Tamanho":
                    for opt in sel.options:
                        t = re.sub(r"\s+", " ", (opt.text or "").strip())
                        m = re.search(r"(\d{1,2})", t)
                        if m and m.group(1)==value:
                            sel.select_by_visible_text(opt.text); break
            time.sleep(0.2); return True
    except Exception:
        return False
    return False

def iterate_children(driver) -> Tuple[List[Dict], List[Dict]]:
    blocks = _find_variation_blocks(driver)
    if not blocks:
        sku, stock = _read_sku_and_stock(driver)
        child = {"sku": sku, "estoque": stock} if sku else {}
        return [], ([child] if child else [])

    labels=[]; options=[]; metas=[]
    for b in blocks:
        lab, opts, meta = _list_group_options_text(driver, b)
        if not opts: continue
        labels.append(lab); options.append(opts); metas.append(meta)

    if not labels:
        sku, stock = _read_sku_and_stock(driver)
        child = {"sku": sku, "estoque": stock} if sku else {}
        return [], ([child] if child else [])

    from itertools import product
    children=[]
    for combo in product(*options):
        for lab, val, meta in zip(labels, combo, metas):
            _select_option(driver, lab, meta, val)
        WebDriverWait(driver, 8, poll_frequency=0.2).until(lambda d: _read_sku_and_stock(d)[0])
        sku, stock = _read_sku_and_stock(driver)
        ch = {"sku": sku, "estoque": stock}
        for lab, val in zip(labels, combo):
            ch[lab] = val
        children.append(ch)

    variations = [{"atributo": lab, "opcoes": ops} for lab, ops in zip(labels, options)]
    return variations, children

# ============================== Consolidação / I/O ==============================
def consolidate_by_product_id(items: List[Dict]) -> List[Dict]:
    by: Dict[str, Dict] = {}
    for it in items:
        if not it: continue
        pid = product_base_id(it.get("url","")) or it.get("sku_base") or it.get("url")
        if pid not in by:
            ref = dict(it)
            ref.setdefault("categories", []); ref.setdefault("images", [])
            ref.setdefault("variations", []); ref.setdefault("children", [])
            ref.setdefault("materials", [])
            by[pid] = ref
        else:
            ref = by[pid]
            ref["categories"] = unique((ref.get("categories") or []) + (it.get("categories") or []))
            ref["images"]     = unique((ref.get("images") or []) + (it.get("images") or []))
            ref["materials"]  = unique((ref.get("materials") or []) + (it.get("materials") or []))
            map_exist = {v["atributo"]: list(v.get("opcoes", [])) for v in (ref.get("variations") or [])}
            for v in (it.get("variations") or []):
                a = v.get("atributo"); ops = v.get("opcoes", [])
                if not a: continue
                if a not in map_exist: map_exist[a] = []
                for o in ops:
                    if o not in map_exist[a]: map_exist[a].append(o)
            ref["variations"] = [{"atributo": k, "opcoes": map_exist[k]} for k in map_exist]
            if not ref.get("description") and it.get("description"): ref["description"] = it["description"]
            if not ref.get("title") and it.get("title"):             ref["title"] = it["title"]
            if not ref.get("sku_base") and it.get("sku_base"):       ref["sku_base"] = it["sku_base"]

            def key_child(c: Dict) -> str:
                if c.get("sku"): return f"SKU::{c['sku']}"
                attrs = {k: v for k, v in c.items() if k not in {"sku","estoque"}}
                return "ATTRS::" + json.dumps(attrs, sort_keys=True, ensure_ascii=False)

            ch_map = { key_child(c): idx for idx, c in enumerate(ref.get("children") or []) }
            for ch in (it.get("children") or []):
                k = key_child(ch)
                if k in ch_map:
                    r = ref["children"][ch_map[k]]
                    s1, s2 = r.get("estoque"), ch.get("estoque")
                    if isinstance(s2, int) and not isinstance(s1, int): r["estoque"] = s2
                    elif isinstance(s1, int) and isinstance(s2, int) and s2 > s1: r["estoque"] = s2
                    for kk, vv in ch.items():
                        if kk == "estoque": continue
                        if kk not in r and vv is not None: r[kk] = vv
                else:
                    ref["children"].append(ch)

    for ref in by.values():
        for ch in ref.get("children", []):
            if not isinstance(ch.get("estoque"), int):
                ch["estoque"] = 0
    return list(by.values())

def save_products_json(items: List[Dict], path=OUT_JSON):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    log.info("Salvo %d produtos em %s", len(items), path)

# ============================== Pipeline principal ==============================
def run_scrape_and_save(headless: bool = True) -> List[Dict]:
    gecko_path = GeckoDriverManager().install()
    login_driver, cookies, localstorage = login_and_collect_auth(gecko_path, headless=headless)

    all_jobs: List[Tuple[str,str]] = []
    for cname, curl in CATEGORIES.items():
        log.info("Categoria: %s (%s)", cname, curl)
        links = collect_links_category_and_subs(login_driver, curl)
        log.info("Links (dedupe por produto) em %s: %d", cname, len(links))
        all_jobs += [(u, cname) for u in links]

    try: login_driver.quit()
    except Exception: pass

    job_q: Queue = Queue()
    for job in all_jobs: job_q.put(job)

    results: List[Dict] = []
    res_lock = threading.Lock()
    retry_later: List[Tuple[str,str]] = []

    workers = [Worker(wid=i+1, gecko_path=gecko_path, cookies=cookies, localstorage=localstorage,
                      job_q=job_q, out_list=results, out_lock=res_lock, retry_list=retry_later,
                      headless=headless)
               for i in range(N_WORKERS)]
    t0 = time.perf_counter()
    for w in workers: w.start()
    for w in workers: w.join()
    dt = time.perf_counter()-t0
    log.info("Processados %d itens com %d workers em %.1fs (≈%.2fs/it)", len(results), N_WORKERS, dt, (dt/len(results) if results else 0.0))

    if ENABLE_SLOW_RETRY and retry_later:
        log.info("Reprocessando %d URLs problemáticos em modo lento...", len(retry_later))
        slow = new_driver(gecko_path, headless=headless)
        prime_auth_on_driver(slow, cookies, localstorage)
        rng = random.Random(42)
        fixed: List[Dict] = []
        for i, (url, cat) in enumerate(retry_later, 1):
            try:
                if REFERER_HOP_ON_RETRY:
                    slow.get(BASE + "/"); time.sleep(0.55 + rng.random()*0.45)
                slow.get(url); time.sleep(0.75 + rng.random()*0.45)
                wait_for_product_ready(slow, timeout=2.0)
                html = safe_page_source(slow)
                if html:
                    base_item = parse_title_desc_imgs(html, url, cat)
                    try: variations, children = iterate_children(slow)
                    except Exception: variations, children = [], []
                    from os.path import commonprefix
                    skus = [c.get("sku") for c in children if c.get("sku")]
                    sku_base = commonprefix(skus) if skus else None
                    base_item.variations = variations; base_item.children = children; base_item.sku_base = sku_base
                    if base_item.title or base_item.description or base_item.children:
                        fixed.append(asdict(base_item))
                time.sleep(0.45 + rng.random()*0.35)
                if i % 50 == 0: log.info("  [retry lento] %d/%d", i, len(retry_later))
            except Exception as e:
                log.warning("Falha no retry lento %s: %s", url, e)
        try: slow.quit()
        except Exception: pass
        results += fixed

    consolidated = consolidate_by_product_id(results)
    log.info("Total consolidados: %d", len(consolidated))
    save_products_json(consolidated, OUT_JSON)
    return consolidated

# ============================== Worker (scraping) ==============================
class Worker(threading.Thread):
    def __init__(self, wid: int, gecko_path: str, cookies: List[dict], localstorage: Dict[str,str],
                 job_q: Queue, out_list: list, out_lock: threading.Lock, retry_list: list,
                 headless=True):
        super().__init__(daemon=True)
        self.wid = wid
        self.gecko_path = gecko_path
        self.cookies = cookies
        self.localstorage = localstorage
        self.job_q = job_q
        self.out_list = out_list
        self.out_lock = out_lock
        self.retry_list = retry_list
        self.headless = headless
        self.driver = None
        self.logger = logging.getLogger(f"worker{wid}")
        self.rng = random.Random(1000 + wid)

    def _get_with_retries(self, url: str) -> Optional[str]:
        tries = 0
        while tries < RETRY_MAX_TRIES:
            if tries > 0 and REFERER_HOP_ON_RETRY:
                try:
                    self.driver.get(BASE + "/"); time.sleep(0.25 + self.rng.random()*0.35)
                except Exception: pass
            try:
                self.driver.get(url)
            except TimeoutException:
                self.logger.debug("Page load timeout (eager), seguindo waits…")

            time.sleep(AFTER_NAV_DELAY_S + self.rng.random()*0.10)
            try: wait_for_product_ready(self.driver, timeout=PRODUCT_READY_TIMEOUT_S)
            except Exception: pass

            html = safe_page_source(self.driver)
            if html: return html

            tries += 1
            back = RETRY_BACKOFF_BASE * (2 ** (tries-1)) + self.rng.random()*0.45
            self.logger.warning("Sem HTML útil em %s (tentativa %d). Backoff %.2fs...", url, tries, back)
            time.sleep(back)
        return None

    def run(self):
        try:
            self.driver = new_driver(self.gecko_path, headless=self.headless)
            try:
                self.driver.get(BASE + "/"); time.sleep(0.2)
            except Exception:
                pass
            try:
                prime_auth_on_driver(self.driver, self.cookies, self.localstorage)
            except Exception:
                pass
            processed = 0; t0 = time.perf_counter()
            while True:
                try:
                    url, cat = self.job_q.get(timeout=5)
                except Empty:
                    break
                if url is None:
                    break
                try:
                    html = self._get_with_retries(url)
                    if html is None:
                        self.retry_list.append((url, cat))
                    else:
                        base_item = parse_title_desc_imgs(html, url, cat)
                        try:
                            variations, children = iterate_children(self.driver)
                        except Exception as e:
                            self.logger.error("Falha ao iterar variações em %s: %s", url, e)
                            variations, children = [], []
                        from os.path import commonprefix
                        skus = [c.get("sku") for c in children if c.get("sku")]
                        sku_base = commonprefix(skus) if skus else None
                        base_item.variations = variations; base_item.children = children; base_item.sku_base = sku_base
                        if not (base_item.title or base_item.description or base_item.children):
                            self.retry_list.append((url, cat))
                        else:
                            with self.out_lock:
                                self.out_list.append(asdict(base_item))
                except Exception as e:
                    self.logger.error("Erro em %s: %s", url, e); self.retry_list.append((url, cat))
                finally:
                    processed += 1
                    if processed % 50 == 0:
                        avg = (time.perf_counter() - t0) / processed
                        self.logger.info("[+%d] ritmo≈%.2fs/it", processed, avg)
        finally:
            try:
                if self.driver:
                    self.driver.quit()
            except Exception:
                pass

# ============================== CLI ==============================
if __name__ == "__main__":
    import argparse, logging, time, json

    parser = argparse.ArgumentParser(description="Scraper Zarpellon — somente scraping + JSON")
    # Execução
    parser.add_argument("--loop", action="store_true", help="Repetir scraping até Ctrl+C.")
    parser.add_argument("--interval", type=int, default=30, help="Intervalo entre ciclos, em minutos (default=30).")
    parser.add_argument("--headless", action="store_true", default=True, help="Rodar navegador headless (default).")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Mostrar janela do navegador.")
    # Scraping/persistência
    parser.add_argument("--out-json", default=OUT_JSON, help="Arquivo JSON de saída (default=produtos_scrape.json).")
    parser.add_argument("--workers", type=int, default=N_WORKERS, help="Workers de scraping (default=4).")

    args = parser.parse_args()
    OUT_JSON  = args.out_json
    N_WORKERS = max(1, int(args.workers))

    def one_cycle():
        # esta função deve existir no seu arquivo — ela roda o scraping e já chama save_products_json(...)
        data = run_scrape_and_save(headless=args.headless)
        logging.info("Scraping concluído com %d produtos (gravados em %s).", len(data), OUT_JSON)

    try:
        if args.loop:
            ciclo = 1
            while True:
                logging.info(f"=== Iniciando ciclo #{ciclo} ===")
                try:
                    one_cycle()
                except Exception:
                    logging.exception("Falha no ciclo — seguirá para o próximo.")
                logging.info(f"=== Ciclo #{ciclo} concluído. Aguardando {args.interval} min (Ctrl+C para sair) ===")
                ciclo += 1
                time.sleep(args.interval * 60)
        else:
            one_cycle()
    except KeyboardInterrupt:
        logging.info("Encerrado pelo usuário (Ctrl+C).")
