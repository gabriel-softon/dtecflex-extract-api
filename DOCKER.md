# 🐳 Guia de Uso do Docker

Este projeto utiliza Docker e Docker Compose para facilitar o desenvolvimento e garantir consistência entre ambientes.

## 📋 Pré-requisitos

- Docker Engine 20.10+
- Docker Compose 2.0+

## 🚀 Como Usar

### Iniciar todos os serviços (Recomendado)

Use o script `docker-start.sh` que mostra os links de acesso automaticamente:

```bash
./docker-start.sh
```

Ou use diretamente o docker-compose:

```bash
docker-compose up -d
```

Isso irá iniciar:
- **Redis** (porta 6380 externa) - Broker para o Celery
- **API FastAPI** (porta 7373) - Backend da aplicação
- **Celery Worker** - Processador de tarefas assíncronas
- **Frontend Angular** (porta 4200) - Interface do usuário

### 📍 Links de Acesso

Após iniciar os serviços, você pode acessar:

- **Frontend (Angular)**: http://localhost:4200
- **API (FastAPI)**: http://localhost:7373
- **Documentação da API (Swagger)**: http://localhost:7373/api/docs
- **Redis**: localhost:6380 (porta externa)

### Ver logs em tempo real

```bash
# Todos os serviços
docker-compose logs -f

# Apenas um serviço específico
docker-compose logs -f api
docker-compose logs -f celery
docker-compose logs -f frontend
```

### Parar os serviços

```bash
docker-compose down
```

### Reconstruir as imagens

Se você alterar dependências (pyproject.toml ou package.json), reconstrua:

```bash
docker-compose build --no-cache
docker-compose up -d
```

### Acessar os serviços

- **Frontend**: http://localhost:4200
- **API**: http://localhost:7373
- **API Docs (Swagger)**: http://localhost:7373/api/docs
- **Redis**: localhost:6379

## 🔧 Configuração de Variáveis de Ambiente

Para configurar variáveis de ambiente (banco de dados, chaves SSH, etc.), você pode:

1. **Criar um arquivo `.env`** na raiz do projeto
2. **Ou editar diretamente** o `docker-compose.yml` na seção `environment` de cada serviço

Exemplo de variáveis que podem ser necessárias (conforme `config/celery.py`):

```env
DB_HOST=mysql
DB_USER=usuario
DB_PASS=senha
DB_NAME=banco
DB_PORT=3306
CELERY_BROKER_URL=redis://redis:6379/0
CELERY_RESULT_BACKEND=redis://redis:6379/1
```

## 📝 Notas Importantes

### Hot Reload

Os volumes estão configurados para permitir hot-reload:
- Alterações no código do backend serão refletidas automaticamente (uvicorn --reload)
- Alterações no código do frontend serão refletidas automaticamente (ng serve)

### Proxy do Frontend

O frontend está configurado para fazer proxy das requisições `/api` para o serviço da API dentro da rede Docker. 

**🔧 Descoberta Dinâmica de IP:**
O sistema usa um script de entrypoint (`docker-entrypoint.sh`) que **descobre automaticamente o IP do container da API** antes de iniciar o Angular. Isso garante que o projeto funcione em **qualquer computador**, sem necessidade de configurar IPs manualmente.

O script tenta descobrir o IP através de múltiplos métodos:
1. `getent hosts` (usando links do Docker)
2. `ping` (extraindo IP da resposta)
3. `nslookup` (resolução DNS)
4. Variável de ambiente `API_CONTAINER_IP` (para testes)

O arquivo `proxy.conf.json` é gerado dinamicamente dentro do container com o IP correto.

### Estrutura de Serviços

- **api**: Roda o servidor FastAPI (uvicorn)
- **celery**: Roda o worker do Celery para processar tarefas assíncronas
- **redis**: Broker e backend de resultados do Celery
- **frontend**: Servidor de desenvolvimento do Angular

## 🐛 Troubleshooting

### Porta já em uso

Se alguma porta estiver em uso, você pode alterar no `docker-compose.yml`:

```yaml
ports:
  - "NOVA_PORTA:PORTA_INTERNA"
```

### Limpar tudo e recomeçar

```bash
docker-compose down -v  # Remove volumes também
docker-compose build --no-cache
docker-compose up -d
```

### Verificar se os containers estão rodando

```bash
docker-compose ps
```

