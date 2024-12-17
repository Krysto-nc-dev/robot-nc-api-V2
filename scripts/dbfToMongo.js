import path from "path";
import fs from "fs";
import { DBFFile } from "dbffile";
import dotenv from "dotenv";
import colors from "colors";
import cliProgress from "cli-progress";
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
    console.log(colors.yellow.bold("\n🔌 Connexion à MongoDB..."));
    await mongoose.connect(mongoUri);
    console.log(colors.green.bold("✅ MongoDB connecté avec succès.\n"));
  } catch (err) {
    console.error(colors.red.bold(`❌ Erreur MongoDB : ${err.message}`));
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
    console.warn(colors.yellow(`⚠️ Impossible de charger le modèle ${modelFileName}: ${err.message}`));
    return null;
  }
};

// Configuration de la barre de progression VERTE
const createProgressBar = (fileName) => {
  return new cliProgress.SingleBar(
    {
      format: `${colors.bold.yellow(fileName)} |${colors.green("{bar}")}| ${colors.green("{value}/{total}")} Enregistrements || {percentage}% || ETA: {eta_formatted}`,
      barCompleteChar: "\u2588", // caractère plein
      barIncompleteChar: "\u2591", // caractère vide
      hideCursor: true,
    },
    cliProgress.Presets.shades_classic
  );
};

// Traitement des fichiers DBF avec décompte par insertion
const processFile = async (filePath, model, fileName, folder) => {
  if (!fs.existsSync(filePath)) {
    console.warn(colors.yellow(`⚠️ Fichier ${fileName}.dbf manquant dans ${folder}`));
    return;
  }

  const dbf = await DBFFile.open(filePath);
  console.log(colors.cyan.bold(`📄 Lecture de ${fileName}.dbf. ${dbf.recordCount} enregistrements.`));

  console.log(colors.yellow(`🗑️ Suppression des anciennes données pour ${fileName}...`));
  await model.deleteMany();

  const progressBar = createProgressBar(fileName);
  progressBar.start(dbf.recordCount, 0, { eta_formatted: "N/A" });

  const records = await dbf.readRecords();
  let insertedCount = 0;

  for (const record of records) {
    const sanitizedRecord = sanitizeRecord(record);
    try {
      await model.create(sanitizedRecord); // Insertion par document
      insertedCount++;
      progressBar.update(insertedCount); // Mise à jour de la barre de progression
    } catch (err) {
      console.error(colors.red(`❌ Erreur d'insertion : ${err.message}`));
    }
  }

  progressBar.stop();
  console.log(
    colors.green.bold(
      `✅ Importation réussie pour ${fileName}. Total inséré : ${colors.green.bold(insertedCount)}/${dbf.recordCount} enregistrements.`
    )
  );
};

// Importation des données DBF
const importDbfsData = async () => {
  console.time("⏱️ Temps total d'exécution");

  const folders = [
    "AVB", "AW", "DQ", "FMB", "HD", "KONE", "KOUMAC", "LD",
    "LE_BROUSSARD", "MEARE", "PAITA_BRICOLAGE", "QC", "SITEC", "VKP",
  ];
  const DBF_FOLDER = path.resolve("./_dbf");

  try {
    await connectDB();

    for (const folder of folders) {
      const folderPath = path.join(DBF_FOLDER, folder);

      if (!fs.existsSync(folderPath)) {
        console.warn(colors.yellow(`⚠️ Dossier introuvable : ${folderPath}`));
        continue;
      }

      console.log(colors.blue.bold(`\n📂 Traitement des fichiers dans le dossier : ${folder}`));

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
          console.log(colors.cyan(`⏱️ Temps écoulé depuis le lancement : ${formatElapsedTime()}`));
        }
      }
    }

    console.log(colors.green.inverse("🎉 Importation complète pour TOUS les dossiers."));
    console.log(colors.cyan(`⏱️ Temps total écoulé : ${formatElapsedTime()}`));
    console.timeEnd("⏱️ Temps total d'exécution");
  } catch (error) {
    console.error(colors.red.bold(`❌ Erreur : ${error.message}`));
    console.log(colors.cyan(`⏱️ Temps écoulé avant l'erreur : ${formatElapsedTime()}`));
    console.timeEnd("⏱️ Temps total d'exécution");
  } finally {
    process.exit();
  }
};

// Exécuter l'importation
importDbfsData();
