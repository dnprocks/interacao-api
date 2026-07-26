import Fastify from "fastify";
import { Queue } from "bullmq";
import Redis from "ioredis";
import { startWorker } from "./worker";

export const redisConnection = new Redis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT) || 6379,
  username: process.env.REDIS_USERNAME || undefined,
  password: process.env.REDIS_PASSWORD || undefined,
  db: Number(process.env.REDIS_DB) || 0,
  maxRetriesPerRequest: null,
});

export const whatsappQueue = new Queue("whatsapp-incoming", {
  connection: redisConnection,
});

const fastify = Fastify({ logger: true });
const DEBOUNCE_MS = 4000; // 4 segundos de espera para ver se o usuário digita mais algo
const port = Number(process.env.PORT ?? 3000);

fastify.get("/", async function handler(request, reply) {
  return "alive!";
});

fastify.post("/api/webhooks/whatsapp", async (request, reply) => {
  console.log("Received WhatsApp webhook:", request.body);

  const payload = request.body as any;
  const value = payload?.entry?.[0]?.changes?.[0]?.value;
  const message = value?.messages?.[0];

  if (!message) {
    return reply.status(200).send({ status: "ignored" });
  }

  const phone = message.from;
  // const bufferKey = `wpp:buffer:${phone}`;
  const jobId = `job:debounce:${phone}`;

  // 1. Armazena o objeto da mensagem dentro de uma lista no Redis (expira em 10min por segurança)
  //   await redisConnection.rpush(bufferKey, JSON.stringify(message));
  //   await redisConnection.expire(bufferKey, 600);

  // 2. Remove o job pendente anterior (se existir) para "zerar o cronômetro"
  //   const existingJob = await whatsappQueue.getJob(jobId);
  //   if (existingJob) {
  // await existingJob.remove().catch(() => {}); // Ignora se o job já começou a rodar
  //   }

  // 3. Agenda o novo job para rodar daqui a 4 segundos
  await whatsappQueue.add(
    "process-buffered-messages",
    { phone },
    {
      jobId, // ID fixo por usuário garante que só existe 1 agendamento ativo por número
      delay: DEBOUNCE_MS,
    },
  );

  // TODO: Implement the logic to handle the incoming WhatsApp message, such as buffering it in Redis and scheduling a job to process it after a debounce period.
  return reply.status(200).send({ status: "buffered" });
});

async function startApp() {
  try {
    const worker = await startWorker();

    const shutdown = async (signal: NodeJS.Signals) => {
      fastify.log.info(`Recebido ${signal}; encerrando aplicação...`);
      await fastify.close();
      await worker.close();
      process.exit(0);
    };

    process.on("SIGINT", () => {
      void shutdown("SIGINT");
    });

    process.on("SIGTERM", () => {
      void shutdown("SIGTERM");
    });

    await fastify.listen({ host: "0.0.0.0", port });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

void startApp();
