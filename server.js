require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const cookieParser = require('cookie-parser');
const mongoose = require('mongoose');
const OpenAI = require("openai");
const { MongoClient } = require('mongodb');
const twilio = require('twilio');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;

// Configuration OpenAI
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY
});

// Exemple de token stocké en variable d'env (ou config)
const token = process.env.WHATSAPP_CLOUD_API_TOKEN || "TON_TOKEN_PERMANENT";
const whatsappPhoneNumberId = process.env.WHATSAPP_PHONE_NUMBER_ID || "TON_PHONE_NUMBER_ID";

// Configuration MongoDB
const mongoUri = process.env.MONGODB_URI;
if (!mongoUri) {
    console.error("❌ Erreur : MONGODB_URI n'est pas défini dans les variables d'environnement.");
    process.exit(1);
}

let db;  // Variable pour stocker la connexion à MongoDB

async function connectToMongoDB() {
  try {
    const mongoClient = new MongoClient(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true });
    await mongoClient.connect();
    db = mongoClient.db('chatbotDB');
    console.log("✅ Connecté à MongoDB avec succès !");
  } catch (err) {
    console.error("❌ Erreur lors de la connexion à MongoDB :", err);
    process.exit(1);
  }
}

// Appel de la connexion MongoDB
connectToMongoDB();

// Middleware
app.use(cors({
  origin: 'https://www.puravivecoach.com', // Remplace par l'URL de ton front-end si nécessaire
  credentials: true
}));
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
app.use(cookieParser());
app.use(express.json()); // parse le JSON entrant de Meta

// Exportation de `db` pour pouvoir l'utiliser ailleurs
module.exports = { db };

let calendar;

// Fonction pour récupérer ou créer un thread
async function getOrCreateThreadId(userNumber) {
  try {
    const collection = db.collection('threads1');
    let thread = await collection.findOne({ userNumber });
    if (!thread) {
      const threadResponse = await openai.beta.threads.create();
      const threadId = threadResponse.id;

      await collection.insertOne({ userNumber, threadId, responses: [] });
      return threadId;
    }
    return thread.threadId;
  } catch (error) {
    console.error('Erreur lors de la récupération ou création du thread:', error);
    throw error;
  }
}

// Fonction pour interagir avec OpenAI
async function interactWithAssistant(userMessage, userNumber) {
    if (!userMessage || userMessage.trim() === "") {
      throw new Error("Le contenu du message utilisateur est vide ou manquant.");
    }
  
    try {
      const threadId = await getOrCreateThreadId(userNumber);
      const currentDateTime = new Date().toLocaleString('es-ES', { timeZone: 'America/Bogota' });
  
      // Envoi du message utilisateur à OpenAI
      await openai.beta.threads.messages.create(threadId, {
        role: "user",
        content: `Mensaje del cliente: "${userMessage}". Nota: El número WhatsApp del cliente es ${userNumber}. Fecha y hora del mensaje: ${currentDateTime}`
      });
  
      // Création d'un nouveau "run" pour générer la réponse
      const runResponse = await openai.beta.threads.runs.create(threadId, {
        assistant_id: "asst_7gcQiaUIhHn6P9ts1te0Fzpo" // Remplace par ton assistant_id
      });
  
      const runId = runResponse.id;
      // Attente de la fin du run ou d'un éventuel function calling
      const messages = await pollForCompletion(threadId, runId);
  
      console.log("📩 Messages reçus de l'assistant :", messages);
  
      // Sauvegarde des messages et du thread dans MongoDB
      if (messages) {
        const collection = db.collection('threads1');
        await collection.updateOne(
          { userNumber },
          {
            $set: { threadId },
            $push: {
              responses: {
                userMessage,
                assistantResponse: messages,
                timestamp: new Date()
              }
            }
          },
          { upsert: true }
        );
      }
  
      return messages;
    } catch (error) {
      console.error("❌ Erreur lors de l'interaction avec l'assistant:", error);
      throw error;
    }
  }  

// Vérification du statut d'un run
async function pollForCompletion(threadId, runId) {
  return new Promise((resolve, reject) => {
    const interval = 2000;
    const timeoutLimit = 80000;
    let elapsedTime = 0;

    const checkRun = async () => {
      try {
        const runStatus = await openai.beta.threads.runs.retrieve(threadId, runId);
        console.log(`📊 Estado del run: ${runStatus.status}`);

        if (runStatus.status === 'completed') {
          const messages = await fetchThreadMessages(threadId);
          console.log("📩 Réponse finale de l'assistant:", messages);
          resolve(messages);
          return;
        }

        if (runStatus.status === 'requires_action' &&
            runStatus.required_action?.submit_tool_outputs?.tool_calls) {
          const toolCalls = runStatus.required_action.submit_tool_outputs.tool_calls;

          for (const toolCall of toolCalls) {
            let params;
            try {
              params = JSON.parse(toolCall.function.arguments);
            } catch (error) {
              console.error("❌ Erreur en parsant les arguments JSON:", error);
              reject(error);
              return;
            }

            if (toolCall.function.name === "get_image_url") {
              console.log("🖼️ Demande d'URL image reçue:", params);
              const imageUrl = await getImageUrl(params.imageCode);

              const toolOutputs = [{
                tool_call_id: toolCall.id,
                output: JSON.stringify({ imageUrl })
              }];

              await openai.beta.threads.runs.submitToolOutputs(threadId, runId, {
                tool_outputs: toolOutputs
              });

              setTimeout(checkRun, 500);
              return;
            } else {
              console.warn(`⚠️ Fonction non gérée (hors MVP): ${toolCall.function.name}`);
              setTimeout(checkRun, 500);
              return;
            }
          }
        } else {
          elapsedTime += interval;
          if (elapsedTime >= timeoutLimit) {
            console.error("⏳ Timeout (80s), annulation du run...");
            await openai.beta.threads.runs.cancel(threadId, runId);
            reject(new Error("Run annulé après 80s sans réponse."));
            return;
          }

          setTimeout(checkRun, interval);
        }

      } catch (error) {
        console.error("Erreur dans pollForCompletion:", error);
        reject(error);
      }
    };

    checkRun();
  });
}

// Récupérer les messages d'un thread
async function fetchThreadMessages(threadId) {
  try {
    const messagesResponse = await openai.beta.threads.messages.list(threadId);
    const messages = messagesResponse.data.filter(msg => msg.role === 'assistant');

    const latestMessage = messages[0];
    let textContent = latestMessage.content
      .filter(c => c.type === 'text')
      .map(c => c.text.value)
      .join(" ");

    // Extraction des URLs Markdown du texte
    const markdownUrlRegex = /!\[.*?\]\((https?:\/\/[^\s)]+)\)/g;
    let match;
    const markdownImageUrls = [];

    while ((match = markdownUrlRegex.exec(textContent)) !== null) {
      markdownImageUrls.push(match[1]);
    }

    // Nettoyage des URL markdown du texte
    textContent = textContent.replace(markdownUrlRegex, '').trim();

    // Suppression des références internes 【XX:XX†nomfichier.json】
    textContent = textContent.replace(/【\d+:\d+†[^\]]+】/g, '').trim();

    // Fonction de conversion Markdown OpenAI → Markdown WhatsApp
    function convertMarkdownToWhatsApp(text) {
      return text
        .replace(/\*\*(.*?)\*\*/g, '*$1*')          // Gras: **texte** → *texte*
        .replace(/\*(.*?)\*/g, '_$1_')              // Italique: *texte* → _texte_
        .replace(/~~(.*?)~~/g, '~$1~')              // Barré: ~~texte~~ → ~texte~
        .replace(/!\[.*?\]\((.*?)\)/g, '')          // Suppression images markdown
        .replace(/\[(.*?)\]\((.*?)\)/g, '$1 : $2')  // Liens markdown → texte : URL
        .replace(/^>\s?(.*)/gm, '$1')               // Citations markdown supprimées
        .replace(/^(\d+)\.\s/gm, '- ')              // Listes numérotées → tirets
        .trim();
    }

    // Application de la conversion Markdown
    textContent = convertMarkdownToWhatsApp(textContent);

    // Récupération des images issues du Function Calling
    const toolMessages = messagesResponse.data.filter(msg => msg.role === 'tool');
    const toolImageUrls = toolMessages
      .map(msg => {
        try {
          return JSON.parse(msg.content[0].text.value).imageUrl;
        } catch {
          return null;
        }
      })
      .filter(url => url != null);

    // Fusion des deux sources d'images (Markdown + Function Calling)
    const images = [...markdownImageUrls, ...toolImageUrls];

    return {
      text: textContent,
      images: images
    };
  } catch (error) {
    console.error("Erreur lors de la récupération des messages du thread:", error);
    return { text: "", images: [] };
  }
}

// Fonction pour récupérer les URLs des images depuis MongoDB
async function getImageUrl(imageCode) {
  try {
    const image = await db.collection("images").findOne({ _id: imageCode });
    return image ? image.url : null;
  } catch (error) {
    console.error("Erreur récupération URL image:", error);
    return null;
  }
}

// Modification du endpoint WhatsApp pour gérer les images
app.post('/whatsapp', async (req, res) => {
  console.log('📩 Requête reçue :', JSON.stringify(req.body, null, 2));

  try {
    const entry = req.body?.entry?.[0];
    const changes = entry?.changes?.[0];
    const value = changes?.value;
    const field = changes?.field;

    // 🚫 Ignorer les événements qui ne sont pas de type "messages"
    if (field !== "messages" || !value.messages || !value.messages[0]) {
      return res.status(200).send("Pas un message entrant à traiter.");
    }

    const message = value.messages[0];
    const from = message.from;
    const phoneNumberId = value.metadata.phone_number_id;
    const messageId = message.id;

    // ✅ Vérifier si ce message a déjà été traité
    const alreadyProcessed = await db.collection('processedMessages').findOne({ messageId });
    if (alreadyProcessed) {
      console.log("⚠️ Message déjà traité, on ignore :", messageId);
      return res.status(200).send("Message déjà traité.");
    }
    await db.collection('processedMessages').insertOne({ messageId });

    // 🧠 Extraire le contenu utilisateur
    let userMessage = '';
    if (message.type === 'text' && message.text.body) {
      userMessage = message.text.body.trim();
    } else if (message.type === 'image') {
      userMessage = "Cliente envió una imagen.";
    } else if (message.type === 'audio') {
      userMessage = "Cliente envió un audio.";
    } else {
      userMessage = "Cliente envió un type de message non géré.";
    }

    if (!userMessage) {
      return res.status(200).send('Message vide ou non géré.');
    }

    // 🔄 Envoyer le message à l’assistant
    const response = await interactWithAssistant(userMessage, from);
    const { text, images } = response;

    // 📤 Répondre via l'API WhatsApp Cloud
    const apiUrl = `https://graph.facebook.com/v16.0/${phoneNumberId}/messages`;
    const headers = {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };

    // 🗣️ Envoi du texte
    if (text) {
      await axios.post(
        apiUrl,
        {
          messaging_product: 'whatsapp',
          to: from,
          text: { body: text },
        },
        { headers }
      );
    }

    // 🖼️ Envoi des images (fonction calling ou markdown)
    if (images && images.length > 0) {
      for (const url of images) {
        if (url) {
          await axios.post(
            apiUrl,
            {
              messaging_product: 'whatsapp',
              to: from,
              type: 'image',
              image: { link: url },
            },
            { headers }
          );
        }
      }
    }

    res.status(200).send('Message traité avec succès.');

  } catch (error) {
    console.error("❌ Erreur lors du traitement du message WhatsApp :", error);
    res.status(500).json({ error: "Erreur serveur." });
  }
});

app.get('/whatsapp', (req, res) => {
  // Récupère les paramètres que Meta envoie
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  // Compare le token reçu avec celui que vous avez défini dans Meta for Developers
  if (mode === 'subscribe' && token === 'myVerifyToken123') {
    console.log('WEBHOOK_VERIFIED');
    // Renvoyer challenge pour confirmer la vérification
    res.status(200).send(challenge);
  } else {
    // Token ou mode invalide
    res.sendStatus(403);
  }
});

// Endpoint de vérification
app.get('/', (req, res) => {
  res.send('Le serveur est opérationnel !');
});

// Lancer le serveur
app.listen(PORT, () => {
  console.log(`Le serveur fonctionne sur le port ${PORT}`);
});
