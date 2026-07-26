import { Worker, Job } from "bullmq";
import Redis from "ioredis";

export const redisConnection = new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT) || 6379,
  username: process.env.REDIS_USERNAME || undefined,
  password: process.env.REDIS_PASSWORD || undefined,
  db: Number(process.env.REDIS_DB) || 0,
  maxRetriesPerRequest: null,
});

export async function startWorker() {
  console.log("[worker] pronto para processar jobs");
  return whatsappWorker;
}

export const whatsappWorker = new Worker(
  "whatsapp-incoming",
  async (job: Job) => {
    console.log(
      `[Job ${job.id}] Iniciando processamento de mensagens para ${job.data.phone}`,
    );
    const { phone } = job.data;
    const bufferKey = `wpp:buffer:${phone}`;

    // 1. Busca e remove (atomicamente) todas as mensagens acumuladas do Redis
    // Usamos pipeline/multi para ler todas as mensagens e apagar a chave em seguida
    const pipeline = redisConnection.pipeline();
    pipeline.lrange(bufferKey, 0, -1);
    pipeline.del(bufferKey);

    const results = await pipeline.exec();
    const rawMessages = (results?.[0]?.[1] as string[]) || [];

    if (rawMessages.length === 0) {
      return;
    }

    // 2. Parseia as mensagens acumuladas
    const messages = rawMessages.map((msg) => JSON.parse(msg));

    console.log(
      `[Job ${job.id}] Processando lote de ${messages.length} mensagem(ns) de ${phone}`,
    );

    // 3. Concatena os textos em um único prompt consolidado
    const textParts: string[] = [];
    let hasAudioOrImage = false;

    for (const msg of messages) {
      if (msg.type === "text") {
        textParts.push(msg.text.body);
      } else if (msg.type === "audio" || msg.type === "voice") {
        // Se houver áudio, trata a transcrição (como vimos no passo anterior)
        hasAudioOrImage = true;
        // ... transcrição ...
      } else if (msg.type === "image") {
        hasAudioOrImage = true;
        // ... tratamento de imagem ...
      }
    }

    // Texto consolidado (ex: "Olá\nPreciso de ajuda\nCom o meu pedido")
    const fullPrompt = textParts.join("\n");

    // 4. Executa a inteligência (RAG + LLM) apenas 1 vez para todo o bloco!
    const aiResponse = await processLLMWithRAG(phone, fullPrompt);

    // 5. Responde o usuário
    await sendWhatsAppMessage(phone, aiResponse);
  },
  {
    connection: redisConnection,
    concurrency: 5,
  },
);

async function processLLMWithRAG(
  phone: string,
  prompt: string,
): Promise<string> {
  // Chamada única para o seu backend RAG/LLM
  return `Entendi suas mensagens:\n"${prompt}"\n\nComo posso ajudar?`;
}

async function sendWhatsAppMessage(to: string, text: string) {
  // Envio pra Meta Graph API...
  console.log(`Enviando mensagem para ${to}: ${text}`);
}

whatsappWorker.on("ready", () => {
  console.log("Worker ready");
});

whatsappWorker.on("active", (job) => {
  console.log("ACTIVE", job.id);
});

whatsappWorker.on("completed", (job) => {
  console.log("COMPLETED", job.id);
});

whatsappWorker.on("failed", (job, err) => {
  console.error("FAILED", job?.id, err);
});

whatsappWorker.on("error", (err) => {
  console.error("WORKER ERROR", err);
});
