import mongoose from "mongoose";

const connectDB = async () => {
  try {
    console.log("🔌 Connexion à MongoDB...".yellow);
    await mongoose.connect(process.env.MONGO_URI);
    console.log("✅ MongoDB connecté avec succès.".green);
  } catch (err) {
    console.error(`❌ Erreur MongoDB : ${err.message}`.red);
    process.exit(1);
  }
};

export default connectDB;
