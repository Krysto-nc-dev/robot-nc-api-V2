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
  console.error(colors.red("❌ MONGO_URI non défini dans config/config.env."));
  process.exit(1);
}

const ERROR_LOG_FILE = "./error.log";

// Fonction pour logger les erreurs
const logError = (message) => {
  fs.appendFileSync(ERROR_LOG_FILE, `${new Date().toISOString()} - ${message}\n`);
};

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

// Nettoyage des valeurs NaN et suppression des champs dupliqués
const sanitizeRecord = (record) => {
  const sanitized = {};
  for (const [key, value] of Object.entries(record)) {
    if (!sanitized.hasOwnProperty(key)) {
      sanitized[key] = typeof value === "number" && isNaN(value) ? 0 : value;
    }
  }
  return sanitized;
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
    const errorMsg = `⚠️ Impossible de charger le modèle ${modelFileName}: ${err.message}`;
    console.warn(colors.yellow(errorMsg));
    logError(errorMsg);
    return null;
  }
};

// Création de la barre de progression personnalisée
const createProgressBar = (fileName, total) => {
  return new cliProgress.SingleBar(
    {
      format: `${colors.yellow.bold(fileName)} |${colors.blue("{bar}")}| ${colors.green("{value}")}${colors.blue("/{total}")} Enregistrements || {percentage}% || ETA: {eta_formatted}`,
      barCompleteChar: "\u2588",
      barIncompleteChar: "\u2591",
      hideCursor: true,
      etaBuffer: 50,
    },
    cliProgress.Presets.rect
  );
};

// Traitement des fichiers DBF
const processFile = async (filePath, model, fileName, folder) => {
  if (!fs.existsSync(filePath)) {
    const errorMsg = `⚠️ Fichier ${fileName}.dbf manquant dans ${folder}`;
    console.warn(colors.yellow(errorMsg));
    logError(errorMsg);
    return;
  }

  const dbf = await DBFFile.open(filePath);
  console.log(colors.cyan.bold(`📄 Lecture de ${fileName}.dbf. ${dbf.recordCount} enregistrements.`));

  console.log(colors.yellow(`🗑️ Suppression des anciennes données pour ${fileName}...`));
  await model.deleteMany();

  const progressBar = createProgressBar(fileName, dbf.recordCount);
  progressBar.start(dbf.recordCount, 0);

  const records = await dbf.readRecords();
  let insertedCount = 0;

  for (const record of records) {
    const sanitizedRecord = sanitizeRecord(record);
    try {
      await model.create(sanitizedRecord);
      insertedCount++;
      progressBar.update(insertedCount);
    } catch (err) {
      const errorMsg = `❌ Erreur d'insertion dans ${fileName}: ${err.message}`;
      console.error(colors.red(errorMsg));
      logError(errorMsg);
    }
  }

  progressBar.stop();
  console.log(
    colors.green.bold(
      `✅ Importation réussie pour ${fileName}. Total inséré : ${colors.green(insertedCount)}${colors.blue("/")}${dbf.recordCount} enregistrements.`
    )
  );
};

// Fonction principale d'importation des données
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
        const errorMsg = `⚠️ Dossier introuvable : ${folderPath}`;
        console.warn(colors.yellow(errorMsg));
        logError(errorMsg);
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
    logError(`Erreur critique : ${error.message}`);
  } finally {
    process.exit();
  }
};

// Exécuter l'importation
importDbfsData();
