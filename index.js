import dotenv from "dotenv";
import express from "express";
import http from "http";
import mongoose from "mongoose";
import { createClient } from "redis";
import { Server } from "socket.io";
import cron from "node-cron";

dotenv.config();

const MONGODB_URI = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017/realtime-chat";
const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const MESSAGE_TTL_SECONDS = 90 * 60; // 1.5 hours

await mongoose.connect(MONGODB_URI);
const messageSchema = new mongoose.Schema({
  room: String,
  sender: String,
  text: String,
  timestamp: Date,
  meta: mongoose.Schema.Types.Mixed
});
const Message = mongoose.model("Message", messageSchema);

const redisClient = createClient({ url: REDIS_URL });
redisClient.on("error", (err) => console.error("Redis error:", err));
await redisClient.connect();

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "http://localhost:5173",
    methods: ["GET", "POST"],
    credentials: true
  }
});

app.get("/", (req, res) => {
  res.send("<h1>Hello from Realtime Socket Chat Server</h1>");
});

io.on("connection", (socket) => {
  console.log("a user connected", socket.id);

  socket.on("join", async (roomId) => {
    try {
      const filter = roomId ? { room: roomId } : {};
      const previousMessages = await Message.find(filter).sort({ timestamp: 1 }).lean();
      socket.emit("previous-messages", previousMessages);
    } catch (error) {
      console.error("Failed to load previous messages:", error);
      socket.emit("error", { message: "Unable to load chat history." });
    }

    if (roomId) {
      socket.join(roomId);
    }
  });

  socket.on("leave", (roomId) => {
    if (roomId) {
      socket.leave(roomId);
    }
  });

  socket.on("send", async (message) => {
    try {
      const payload = {
        room: message.room || "global",
        sender: message.sender || "anonymous",
        text: message.text || message.message || "",
        timestamp: message.timestamp ? new Date(message.timestamp) : new Date(),
        meta: message.meta || {}
      };

      const redisKey = `messages:${payload.room}`;
      const serialized = JSON.stringify({
        ...payload,
        timestamp: payload.timestamp.toISOString()
      });

      await redisClient.rPush(redisKey, serialized);
      await redisClient.expire(redisKey, MESSAGE_TTL_SECONDS);

      console.log("saved message to redis", payload);
      socket.to(payload.room).emit("message", payload);
    } catch (error) {
      console.error("Failed to save message to Redis:", error);
      socket.emit("error", { message: "Unable to save message." });
    }
  });
});

cron.schedule("*/1 * * * *", async () => {
  console.log("[cron] syncing redis messages to MongoDB");

  try {
    for await (const key of redisClient.scanIterator({ MATCH: "messages:*" })) {
      const items = await redisClient.lRange(key, 0, -1);
      if (!items.length) {
        continue;
      }

      const docs = items.map((item) => {
        const parsed = JSON.parse(item);
        return {
          room: parsed.room,
          sender: parsed.sender,
          text: parsed.text,
          timestamp: new Date(parsed.timestamp),
          meta: parsed.meta || {}
        };
      });

      if (docs.length) {
        await Message.insertMany(docs, { ordered: false });
        await redisClient.del(key);
        console.log(`[cron] synced ${docs.length} messages from ${key} to MongoDB`);
      }
    }
  } catch (error) {
    console.error("[cron] failed to sync redis messages:", error);
  }
});

server.listen(5050, () => {
  console.log("listening on *:5050");
});
