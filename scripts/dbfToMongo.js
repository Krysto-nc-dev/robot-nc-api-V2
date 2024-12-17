import path from "path";
import fs from "fs";
import { DBFFile } from "dbffile";
import dotenv from "dotenv";
import colors from "colors";
import { SingleBar, Presets } from "cli-progress";
import mongoose from "mongoose";
import { pathToFileURL } from "url";

// Charger les variables d'environnement
dotenv.config({ path: path.resolve("config/config.env") });

const mongoUri = process.env.MONGO_URI || process.env.MONGO_URI_DEV;

if (!mongoUri) {
  console.error("❌ MONGO_URI non défini dans config/config.env.".red);
  process.exit(1);
}

const connectDB = async () => {
  try {
    console.log("🔌 Connexion à MongoDB...".yellow);
    await mongoose.connect(mongoUri);
    console.log("✅ MongoDB connecté avec succès.".green);
  } catch (err) {
    console.error(`❌ Erreur MongoDB : ${err.message}`.red);
    process.exit(1);
  }
};

const sanitizeRecord = (record) => {
  return Object.fromEntries(
    Object.entries(record).map(([key, value]) => [
      key,
      typeof value === "number" && isNaN(value) ? 0 : value,
    ])
  );
};

const loadModel = async (folder) => {
  try {
    const modelPath = pathToFileURL(
      path.resolve(`./models/bases/${folder}/${folder}Article.js`)
    ).href;

    const { default: model } = await import(modelPath);
    return model;
  } catch (err) {
    console.warn(`⚠️ Impossible de charger le modèle pour ${folder}: ${err.message}`.yellow);
    return null;
  }
};

const importDbfsData = async () => {
  const folders = [
    "AVB", "AW", "DQ", "FMB", "HD", "KONE", "KOUMAC", "LD", 
    "LE_BROUSSARD", "MEARE", "PAITA_BRICOLAGE", "QC", "SITEC", "VKP"
  ];
  const DBF_FOLDER = path.resolve("./_dbf"); // Correction pour la racine

  try {
    await connectDB();

    for (const folder of folders) {
      const folderPath = path.join(DBF_FOLDER, folder);

      if (!fs.existsSync(folderPath)) {
        console.warn(`⚠️ Dossier introuvable : ${folderPath}`.yellow);
        continue;
      }

      console.log(`\n📂 Traitement des fichiers dans le dossier : ${folder}`.blue);

      const model = await loadModel(folder);
      if (!model) continue;

      const filePath = path.join(folderPath, "article.dbf");
      if (!fs.existsSync(filePath)) {
        console.warn(`⚠️ Fichier article.dbf manquant dans ${folderPath}`.yellow);
        continue;
      }

      const dbf = await DBFFile.open(filePath);
      console.log(`📄 Lecture de ${filePath}. ${dbf.recordCount} enregistrements.`.green);

      console.log(`🗑️ Suppression des anciennes données pour ${folder}...`.yellow);
      await model.deleteMany();

      const progressBar = new SingleBar(
        { format: `${folder} |{bar}| {percentage}% | {value}/{total} Enregistrements` },
        Presets.shades_classic
      );

      progressBar.start(dbf.recordCount, 0);

      const records = await dbf.readRecords();
      let insertedCount = 0;

      // Insérer les enregistrements par lot pour éviter de tout traiter en une seule fois
      const batchSize = 1000; // Taille des lots
      for (let i = 0; i < records.length; i += batchSize) {
        const batch = records.slice(i, i + batchSize).map(sanitizeRecord);
        const result = await model.insertMany(batch);
        insertedCount += result.length; // Compter les documents insérés
        progressBar.update(insertedCount); // Mettre à jour la barre de progression
      }

      progressBar.stop();
      console.log(
        `✅ Importation réussie pour ${folder}. Total inséré : ${insertedCount} enregistrements.`.green
      );
    }

    console.log("🎉 Importation complète pour TOUS les dossiers.".green.inverse);
    process.exit();
  } catch (error) {
    console.error(`❌ Erreur : ${error.message}`.red.inverse);
    process.exit(1);
  }
};

importDbfsData();
