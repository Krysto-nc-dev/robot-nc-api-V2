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

// Connexion à MongoDB
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

// Nettoyage des valeurs NaN dans les enregistrements
const sanitizeRecord = (record) => {
  return Object.fromEntries(
    Object.entries(record).map(([key, value]) => [
      key,
      typeof value === "number" && isNaN(value) ? 0 : value,
    ])
  );
};

// Fonction pour formater le temps écoulé
const startTime = Date.now();
const formatElapsedTime = () => {
  const elapsedMs = Date.now() - startTime;
  const seconds = Math.floor((elapsedMs / 1000) % 60);
  const minutes = Math.floor((elapsedMs / (1000 * 60)) % 60);
  const hours = Math.floor(elapsedMs / (1000 * 60 * 60));

  return `${hours}h ${minutes}m ${seconds}s`;
};

// Chargement dynamique des modèles
const loadModel = async (folder, modelType) => {
  const modelFileName = {
    article: `${folder}Article`,
    classnum: `${folder}Classnum`,
    fournisseur: `${folder}Fournisseur`,
    client: `${folder}Client`,
    facture: `${folder}Facture`,
    factureDetail: `${folder}FactureDetail`,
    tier: `${folder}Tier`,
  }[modelType];

  try {
    const modelPath = pathToFileURL(
      path.resolve(`./models/bases/${folder}/${modelFileName}.js`)
    ).href;
    const { default: model } = await import(modelPath);
    return model;
  } catch (err) {
    console.warn(`⚠️ Impossible de charger le modèle ${modelFileName}: ${err.message}`.yellow);
    return null;
  }
};

// Traitement des fichiers DBF avec journalisation des erreurs
const processFile = async (filePath, model, fileName, folder) => {
  if (!fs.existsSync(filePath)) {
    console.warn(`⚠️ Fichier ${fileName}.dbf manquant dans ${folder}`.yellow);
    return;
  }

  const dbf = await DBFFile.open(filePath);
  console.log(`📄 Lecture de ${fileName}.dbf. ${dbf.recordCount} enregistrements.`.green);

  console.log(`🗑️ Suppression des anciennes données pour ${fileName}...`.yellow);
  await model.deleteMany();

  const progressBar = new SingleBar(
    {
      format: `${fileName} |{bar}| {percentage}% | {value}/{total} Enregistrements`,
      clearOnComplete: false,
      hideCursor: true,
    },
    Presets.shades_classic
  );

  progressBar.start(dbf.recordCount, 0);

  const records = await dbf.readRecords();
  let insertedCount = 0;
  const batchSize = 1000;

  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize).map(sanitizeRecord);

    try {
      const result = await model.insertMany(batch, { ordered: false });
      insertedCount += result.length;
    } catch (err) {
      console.error(`❌ Erreur d'insertion : ${err.message}`.red);
    }
    progressBar.update(insertedCount);
  }

  progressBar.stop();
  console.log(`✅ Importation réussie pour ${fileName}. Total inséré : ${insertedCount}/${dbf.recordCount} enregistrements.`.green);
};

// Importation des données DBF pour chaque dossier
const importDbfsData = async () => {
  console.time("⏱️ Temps total d'exécution");

  const folders = [
    "AVB", "AW", "DQ", "FMB", "HD", "KONE", "KOUMAC", "LD",
    "LE_BROUSSARD", "MEARE", "PAITA_BRICOLAGE", "QC", "SITEC", "VKP"
  ];
  const DBF_FOLDER = path.resolve("./_dbf");

  try {
    await connectDB();

    for (const folder of folders) {
      const folderPath = path.join(DBF_FOLDER, folder);

      if (!fs.existsSync(folderPath)) {
        console.warn(`⚠️ Dossier introuvable : ${folderPath}`.yellow);
        continue;
      }

      console.log(`\n📂 Traitement des fichiers dans le dossier : ${folder}`.blue);

      const models = {
        article: await loadModel(folder, "article"),
        classnum: await loadModel(folder, "classnum"),
        fournisseur: await loadModel(folder, "fournisseur"),
        client: await loadModel(folder, "client"),
        facture: await loadModel(folder, "facture"),
        factureDetail: await loadModel(folder, "factureDetail"),
        tier: await loadModel(folder, "tier"),
      };

      for (const [fileName, model] of Object.entries(models)) {
        if (model) {
          const fileMap = {
            article: "article.dbf",
            classnum: "classes.dbf",
            fournisseur: "fourniss.dbf",
            client: "clients.dbf",
            facture: "facture.dbf",
            factureDetail: "detail.dbf",
            tier: "tiers.dbf",
          };
          await processFile(path.join(folderPath, fileMap[fileName]), model, fileName, folder);
          console.log(`⏱️ Temps écoulé depuis le lancement : ${formatElapsedTime()}`.cyan);
        }
      }
    }

    console.log("🎉 Importation complète pour TOUS les dossiers.".green.inverse);
    console.log(`⏱️ Temps total écoulé : ${formatElapsedTime()}`.cyan);
    console.timeEnd("⏱️ Temps total d'exécution");
  } catch (error) {
    console.error(`❌ Erreur : ${error.message}`.red.inverse);
    console.log(`⏱️ Temps écoulé avant l'erreur : ${formatElapsedTime()}`.cyan);
    console.timeEnd("⏱️ Temps total d'exécution");
  } finally {
    process.exit();
  }
};

// Exécuter l'importation
importDbfsData();
