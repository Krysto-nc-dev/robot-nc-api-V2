import mongoose from "mongoose";

// Définition du schéma pour le fichier DBF CLASSNUM
const DqClassnumSchema = new mongoose.Schema(
  {
    CLASSNUM: { type: String }, // Code de la classe (2 caractères)
    CLASSLIB: { type: String }, // Libellé de la classe
    COEFF: { type: Number}, // Coefficient principal
    COEFF2: { type: Number}, // Coefficient 2
    COEFF3: { type: Number}, // Coefficient 3
    COEFF4: { type: Number }, // Coefficient 4
    COEFF5: { type: Number  }, // Coefficient 5
    COMPTE: { type: String }, // Numéro de compte (6 caractères)
  },
  { timestamps: true } // Ajoute les champs `createdAt` et `updatedAt`
);

export default mongoose.model("DqClassnum", DqClassnumSchema);
