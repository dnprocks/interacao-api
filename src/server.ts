import Fastify from "fastify";
import { Queue } from "bullmq";
import { startWorker } from "./worker";
import { redisConnection } from "./redis";

export const whatsappQueue = new Queue("whatsapp-incoming", {
  connection: redisConnection,
});

const fastify = Fastify({ logger: true });
const DEBOUNCE_MS = 4000; // 4 segundos de espera para ver se o usuário digita mais algo
const port = Number(process.env.PORT ?? 3000);

function extractWhatsappMessage(payload: any) {
  const metaValue =
    payload?.body?.entry?.[0]?.changes?.[0]?.value ??
    payload?.entry?.[0]?.changes?.[0]?.value;

  const metaMessage = metaValue?.messages?.[0];

  const evolutionMessage =
    payload?.message ??
    payload?.messages?.[0] ??
    payload?.body?.message ??
    payload?.body?.messages?.[0] ??
    payload?.payload?.message ??
    payload?.payload?.messages?.[0] ??
    payload?.data?.message ??
    payload?.data?.messages?.[0];

  return metaMessage ?? evolutionMessage;
}

fastify.get("/", async function handler(request, reply) {
  return "alive!";
});


fastify.post("/api/webhooks/whatsapp", async (request, reply) => {
  console.log("Received WhatsApp webhook:", request.body);

  const payload = request.body as any;
  const message = extractWhatsappMessage(payload);

  if (!message) {
    return reply.status(200).send({ status: "ignored" });
  }

  const phone =
    message?.from ??
    message?.sender ??
    message?.from_number ??
    message?.originator ??
    message?.contact?.phoneNumber ??
    message?.contact?.wa_id ??
    message?.fromWaId ??
    message?.number;

  if (!phone) {
    return reply.status(200).send({ status: "ignored" });
  }

  const bufferKey = `wpp:buffer:${phone}`;
  const jobId = `job:debounce:${phone}`;

  // 1. Armazena o objeto da mensagem dentro de uma lista no Redis (expira em 10min por segurança)
  await redisConnection.rpush(bufferKey, JSON.stringify(message));
  await redisConnection.expire(bufferKey, 600);

  // 2. Remove o job pendente anterior (se existir) para "zerar o cronômetro"
  const existingJob = await whatsappQueue.getJob(jobId);
  if (existingJob) {
    await existingJob.remove().catch(() => {}); // Ignora se o job já começou a rodar
  }

  // 3. Agenda o novo job para rodar daqui a 4 segundos
  const job = await whatsappQueue.add(
    "process-buffered-messages",
    { phone },
    {
      jobId, // ID fixo por usuário garante que só existe 1 agendamento ativo por número
      delay: DEBOUNCE_MS,
    },
  );

  console.log(await job.getState());

  console.log(
    `[Webhook] Agendado job ${job.id} para processar mensagens de ${phone} em ${DEBOUNCE_MS}ms`,
  );
  console.log(`[Webhook] Job ID: ${jobId}, Phone: ${phone}`);

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
