import mongoose from "mongoose";

// Définition du schéma pour le fichier DBF TIERS
const VkpTierSchema = new mongoose.Schema(
  {
    COMPTE: { type: String, required: true }, // Code du compte
    TIERS: { type: Number, default: 0 }, // Numéro de tiers
    NOM: { type: String }, // Nom associé au compte
    DEBIT: { type: Number, default: 0 }, // Montant du débit
    CREDIT: { type: Number, default: 0 }, // Montant du crédit
  },
  { timestamps: true } // Ajoute les champs `createdAt` et `updatedAt`
);

export default mongoose.model("VkpTier", VkpTierSchema);
