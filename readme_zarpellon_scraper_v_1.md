# README — Zarpellon Scraper v1.0 (Somente Scraping)

> **Objetivo**
>
> Extrair (scraping) produtos do site Zarpellon Joias e salvar em um arquivo JSON padronizado. **Este pacote não faz push para o Bling.** Caso o push seja contratado depois, entregamos outro módulo separado.

---

## 1) Visão geral

- Navegador: **Firefox** (controle via Selenium + webdriver-manager)
- Execução: **headless por padrão** (pode mostrar a janela com `--no-headless`)
- Saída: arquivo JSON (ex.: `produtos_scrape.json`) contendo produtos, variações e estoque por SKU.
- Logs: console + `scraper.log` no diretório do projeto.

---

## 2) Requisitos

- **Python 3.10+** (testado em 3.11/3.12/3.13)
- **Firefox** instalado
- Dependências Python (via `requirements.txt`):
  ```bash
  pip install -r requirements.txt
  # ou manualmente
  pip install selenium webdriver-manager beautifulsoup4 lxml python-dotenv requests
  ```

> O **geckodriver** é baixado automaticamente pelo `webdriver-manager`.

---

## 3) Arquivos da entrega

- `zarpellon-scraping-v1.0.py` — script principal (**somente scraping + JSON**)
- `logininfo.env` — arquivo de ambiente **(não versionar)** com credenciais do site da loja
- `requirements.txt` — dependências

---

## 4) Configuração (.env)

Crie/edite um arquivo chamado **`logininfo.env`** na mesma pasta do script com as seguintes variáveis:

```env
ZARPELLON_USER="email@dominio.com"  # Usuário da loja
ZARPELLON_PASS="sua-senha"         # Senha da loja
```

> **Importante:**
>
> - **Não** comitar esse arquivo em repositórios.
> - Variáveis iniciadas por `BLING_` (se existirem) **são ignoradas por este script**. Elas só serão usadas por um módulo de **push Bling** (opcional/futuro).

---

## 5) Como executar

### 5.1 Execução simples (um ciclo)

```bash
# Linux/macOS
python3 zarpellon-scraping-v1.0.py --headless --out-json produtos_scrape.json

# Windows (CMD)
python zarpellon-scraping-v1.0.py --headless --out-json produtos_scrape.json
```

- Ao final, o console indicará quantos produtos foram salvos e **`produtos_scrape.json`** será gerado/atualizado.

### 5.2 Janela visível

```bash
python zarpellon-scraping-v1.0.py --no-headless
```

### 5.3 Modo contínuo (loop)

Roda indefinidamente, esperando X minutos entre ciclos:

```bash
python zarpellon-scraping-v1.0.py --loop --interval 30 --headless
```

### 5.4 Concorrência

Ajuste o número de **workers** (threads) de scraping:

```bash
python zarpellon-scraping-v1.0.py --workers 4
```

> Dica: em máquinas modestas ou se o site estiver sensível, reduza `--workers`.

---

## 6) Categorias e comportamento do scraper

O scraper já vem configurado com as categorias principais do site (Anéis, Brincos, Colares, Conjuntos, Pingentes, Pulseiras, etc.). Para incluir/retirar uma categoria, edite o dicionário de categorias dentro do script.

O fluxo básico por categoria é:
1. Paginação e coleta de links de produtos.
2. Para cada produto: abre a página, garante que elementos essenciais carregaram e coleta **título**, **descrição**, **imagens**, **categorias**, **variações** (atributos/opções) e **filhos** (SKUs com estoque).
3. Consolida numa lista e grava em JSON ao final do ciclo.

Há ainda proteções contra instabilidades: backoff exponencial, limites por categoria, pausa entre páginas e janelas de espera para o carregamento de elementos, com foco em evitar bloqueios do servidor.

---

## 7) Formato do JSON de saída

Cada item de produto contém, tipicamente:

```json
{
  "url": "https://site/produto/1234:5678/NOME-DO-PRODUTO",
  "title": "NOME DO PRODUTO",
  "sku_base": "0112345...",           // prefixo comum, quando identificável
  "description": "Texto descritivo…",
  "images": ["https://.../foto1.jpg", "https://.../foto2.jpg"],
  "categories": ["Anéis"],
  "variations": [                      // lista de atributos disponíveis
    {"atributo": "MATERIAL", "opcoes": ["PRATA LISA"]},
    {"atributo": "Cor",      "opcoes": ["INCOLOR"]},
    {"atributo": "Tamanho",  "opcoes": ["14","16","18","20"]}
  ],
  "children": [                        // combinações (SKUs) com estoque
    {"sku": "0112345...14", "estoque": 5, "MATERIAL": "PRATA LISA", "Cor": "INCOLOR", "Tamanho": "14"},
    {"sku": "0112345...16", "estoque": 2, "MATERIAL": "PRATA LISA", "Cor": "INCOLOR", "Tamanho": "16"}
  ],
  "materials": [],
  "price": null                        // preço não é coletado nesta versão
}
```

> Observações:
>
> - **`children`** lista os SKUs concretos (combinações de variação) e seus **estoques**.
> - **`sku_base`** pode vir preenchido quando o script consegue deduzir um prefixo comum a partir dos SKUs dos filhos.
> - **`price`** permanece `null` nesta versão (não coletamos preço).

---

## 8) Logs e troubleshooting

- Os logs são gravados em **`scraper.log`** além do console.
- Mensagens comuns:
  - *Page load timeout (eager)* → o site demorou; o script re-tenta com backoff.
  - *Sem HTML útil* → re-tenta e aplica backoff.
  - *Falha ao iterar variações* → tenta seguir com o que for possível daquele produto.
- Dicas:
  - Se aparecerem **403** com frequência, reduza `--workers` e aumente o intervalo entre ciclos.
  - Use `--no-headless` para inspecionar visualmente passos da automação.
  - Garanta que **Firefox** está instalado/atualizado.

---

## 9) Agendamento (opcional)

### 9.1 Linux — `cron` (a cada 30 min)
```bash
crontab -e
*/30 * * * * cd /caminho/do/projeto && /usr/bin/python3 zarpellon-scraping-v1.0.py --headless >> cron.out 2>&1
```

### 9.2 Linux — `systemd` (serviço contínuo com `--loop`)
Crie `/etc/systemd/system/zarpellon-scraper.service`:
```ini
[Unit]
Description=Zarpellon Scraper (loop)
After=network-online.target

[Service]
WorkingDirectory=/caminho/do/projeto
ExecStart=/usr/bin/python3 zarpellon-scraping-v1.0.py --loop --interval 30 --headless
Environment=PYTHONUNBUFFERED=1
Restart=always

[Install]
WantedBy=multi-user.target
```

### 9.3 Windows — Agendador (a cada 30 min)
```bat
schtasks /Create /TN "ZarpellonScraper" /SC MINUTE /MO 30 /F ^
  /TR "C:\Python313\python.exe C:\caminho\do\projeto\zarpellon-scraping-v1.0.py --headless" ^
  /ST 08:00
```

---

## 10) Personalizações rápidas

- **Categorias**: editar o dicionário de categorias no topo do script para adicionar/remover URLs de listagem.
- **Arquivo de saída**: alterar via `--out-json meu_arquivo.json`.
- **Workers**: `--workers N` (padrão 4). Comece baixo se o site ficar sensível.
- **Janela**: `--no-headless` para acompanhar o navegador.

---

## 11) Boas práticas de segurança

- Evite compartilhar logs contendo URLs internas de sessão.
- Troque a senha do usuário periodicamente e aplique MFA se o site suportar.

---

## 12) Checklist de instalação rápida

1. Instale Python 3.10+ e Firefox.
2. `pip install -r requirements.txt`
3. Crie `logininfo.env` com `ZARPELLON_USER` e `ZARPELLON_PASS`.
4. Rode: `python zarpellon-scraping-v1.0.py --headless` (ou `--no-headless`).
5. Verifique `produtos_scrape.json` e o `scraper.log`.

---

## 13) Perguntas frequentes (FAQ)

**O script também envia dados para o Bling?**
> Não. Esta versão **somente coleta**. O push para o Bling fica em um módulo separado.

**Como incluir uma nova categoria?**
> Edite o dicionário de categorias no topo do script adicionando a URL da listagem.

**Posso rodar continuamente?**
> Sim, use `--loop --interval MINUTOS` ou configure `cron/systemd/Agendador`.

**Os preços não aparecem; é normal?**
> Sim. Preço não é escopo desta versão.

---

## 14) Suporte

Em caso de dúvidas ou ajustes, descreva:
- Sistema operacional e versão do Python
- Comando executado
- Trecho relevante de `scraper.log`
- Trecho do JSON gerado

Assim conseguimos reproduzir e orientar rapidamente.

