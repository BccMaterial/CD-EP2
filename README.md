# CD-EP2

Scripts de inserção de arquivos CSV e consulta em um banco de dados de grafo Neo4j em python.

## Setup do Ambiente

Para fazer o setup do ambiente, é necessário:
- Criar o arquivo `.env`
- Criar o ambiente virtual (`.venv`)
- Instalar as dependências no `.venv`

### Arquivo .venv

Copie o `.env.example` para `.env`, e preencha os valores

### Ambiente virtual & Dependências

Para Linux, execute:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Para windows, é necessário mudar o caminho do `activate`:
```powershell
python -m venv .venv
.venv/Scripts/Activate.ps1
pip install -r requirements.txt
```
