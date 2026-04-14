import { GoogleGenerativeAI } from "@google/generative-ai";

async function main() {
  try {
    const apiKey = process.env.GOOGLE_API_KEY || process.env.GEMINI_API_KEY;
    if (!apiKey) {
      throw new Error("No API key found. Set GOOGLE_API_KEY or GEMINI_API_KEY.");
    }

    const genAI = new GoogleGenerativeAI(apiKey);

    const model = genAI.getGenerativeModel({
      model: "gemini-2.5-flash",
    });

    const result = await model.generateContent("Write a simple FastAPI example");
    console.log(result.response.text());
  } catch (error) {
    console.error("Gemini error:", error);
  }
}

main();