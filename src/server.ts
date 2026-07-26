import Fastify from "fastify";

const fastify = Fastify({ logger: true });
const DEBOUNCE_MS = 4000; // 4 segundos de espera para ver se o usuário digita mais algo

fastify.get("/", async function handler(request, reply) {
  return "alive!";
});

fastify.post("/api/webhooks/whatsapp", async (request, reply) => {
  const payload = request.body as any;
  const value = payload?.entry?.[0]?.changes?.[0]?.value;
  const message = value?.messages?.[0];

  if (!message) {
    return reply.status(200).send({ status: "ignored" });
  }
  // TODO: Implement the logic to handle the incoming WhatsApp message, such as buffering it in Redis and scheduling a job to process it after a debounce period.
  return reply.status(200).send({ status: "buffered" });
});

(async () => {
  // Run the server!
  try {
    await fastify.listen({ port: 3000 });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
})();
