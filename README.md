# interacao-api

`interacao-api` é um backend Node.js/TypeScript projetado para processar webhooks do WhatsApp Business de forma assíncrona, confiável e escalável.

## Visão geral

Esta aplicação oferece um endpoint de webhook que recebe eventos do WhatsApp Business (via Meta) e os transforma em trabalhos assíncronos com `BullMQ` e `Redis`.

A arquitetura principal do projeto é:

- Um endpoint HTTP para receber webhooks do WhatsApp Business.
- Buffer em Redis por usuário para agrupar mensagens enviadas em sequência.
- Enfileiramento de jobs com `BullMQ` para debouncing e processamento em lote.
- Um worker que consome os jobs, consolida as mensagens e aciona a lógica de IA/LLM.

## Arquitetura e integração com cf-worker-producer

Este projeto faz parte de uma arquitetura maior de ingestão e processamento de mensagens.

No front, recomenda-se usar o projeto [cf-worker-producer](https://github.com/dnprocks/cf-worker-producer) como produtor de fila na borda da Cloudflare. O `cf-worker-producer`:

- recebe os webhooks do WhatsApp Business/Meta,
- realiza o handshake de verificação do Meta,
- responde rapidamente para manter a validação da Meta,
- publica os eventos em uma fila Cloudflare,
- faz entrega assíncrona para o backend VPS configurado via `VPS_WEBHOOK_URL`.

A partir daí, `interacao-api` atua no backend:

- recebe o evento do Worker externo ou de outra fonte confiável,
- armazena as mensagens no Redis,
- agenda o processamento após um curto período de debounce,
- consome o job e dispara o motor de IA/RAG,
- envia a resposta de volta ao usuário via WhatsApp.

Essa separação garante baixa latência no endpoint do Meta, retry automático na entrega e melhor resiliência.

## Componentes principais

- `src/server.ts`: servidor Fastify que expõe o endpoint `POST /api/webhooks/whatsapp` e gerencia o agendamento de jobs.
- `src/worker.ts`: worker BullMQ que processa mensagens agrupadas por telefone.
- `src/redis.ts`: conexão Redis reutilizável.

## Funcionalidades

- Recebimento de webhook do WhatsApp Business.
- Bufferização de mensagens em Redis por número de telefone.
- Debounce para agrupar mensagens rápidas em um único lote.
- Processamento assíncrono com `BullMQ`.
- Stub de integração com IA/RAG e envio de mensagens de retorno.

## Como executar

### Pré-requisitos

- Node.js (recomendado 18+)
- Redis
- Docker e Docker Compose (opcional, para ambiente local)

### Instalação

```bash
npm install
```

### Executar localmente com Docker

```bash
docker compose up --build
```

Isso iniciará:

- Redis em `localhost:6379`
- `interacao-api` em `http://localhost:3000`
- o worker em background dentro do mesmo serviço Docker Compose

### Executar em desenvolvimento sem Docker

```bash
npm run dev
```

Em outro terminal:

```bash
npm run dev:worker
```

## Endpoints

- `GET /` - retorna `alive!`.
- `POST /api/webhooks/whatsapp` - recebe o webhook do WhatsApp Business e agenda o processamento em segundo plano.

### Exemplo de payload

O endpoint atualmente aceita o formato padrão do webhook do WhatsApp Business e extrai o primeiro evento de mensagem para processamento.

## Variáveis de ambiente

- `PORT` - porta do servidor HTTP (padrão: `3000`).
- `REDIS_HOST` - host do Redis (padrão: `127.0.0.1`).
- `REDIS_PORT` - porta do Redis (padrão: `6379`).
- `REDIS_USERNAME` - usuário Redis (opcional).
- `REDIS_PASSWORD` - senha Redis (opcional).
- `REDIS_DB` - banco de dados Redis (padrão: `0`).

## Integração com IA e envio de WhatsApp

O trabalho de processamento em `src/worker.ts` contém as funções `processLLMWithRAG` e `sendWhatsAppMessage` como stubs:

- `processLLMWithRAG(phone, prompt)` deve chamar o backend de IA/RAG ou um serviço de LLM.
- `sendWhatsAppMessage(to, text)` deve enviar a resposta para a API Graph do WhatsApp/Meta.

Essas partes devem ser implementadas conforme o fluxo de negócios da sua aplicação.

## Observações de arquitetura

Com base na arquitetura do sistema, este backend funciona como o componente de processamento e resposta:

- O `cf-worker-producer` na borda Cloudflare atua como produtor de eventos.
- O Redis e BullMQ garantem que mensagens rápidas do mesmo usuário sejam agrupadas antes do processamento.
- O worker consome cada lote e deve se integrar a um motor de IA ou RAG para gerar respostas.
- O fluxo final envia a resposta de volta via Meta/WhatsApp.

## Estrutura do projeto

- `Dockerfile` - imagem Docker para executar a aplicação.
- `docker-compose.yml` - orquestra Redis, API e worker.
- `src/server.ts` - servidor e webhook receiver.
- `src/worker.ts` - worker de processamento de jobs.
- `src/redis.ts` - cliente Redis.

## Próximos passos

- implementar a entrega real ao WhatsApp Graph API,
- adicionar autenticação e validação do webhook de entrada,
- consolidar o pipeline RAG/LLM com o serviço de IA escolhido,
- estender o tratamento de mídias (áudio, imagem) no processo de mensagens.
