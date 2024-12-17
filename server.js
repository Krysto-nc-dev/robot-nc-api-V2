import express from "express";
import dotenv from "dotenv";
import morgan from "morgan";
import connectDB from "./config/db.js";
import cors from "cors";
import colors from "colors";
import errorHandler from "./middleware/error.js"; // Middleware d'erreurs


// Route Files
import bootcamps from './routes/bootcamps.js'




// Configuration des variables d'environnement
dotenv.config({path: './config/config.env'});



const app = express()

// Body parser 
app.use(express.json())


// Connexion à la base de données MongoDB
connectDB();



// Configuration des logs pour le mode développement
if (process.env.NODE_ENV === "development") {
    app.use(morgan("dev"));
  }

// Mount Routers 

app.use('/api/v2/bootcamps', bootcamps)

// Gestionnaire d'erreurs global (toujours en dernier !)
app.use(errorHandler);



const PORT = process.env.PORT || 5000

app.listen(PORT, console.log(`Server running in ${process.env.NODE_ENV} mode on port ${PORT}`))
