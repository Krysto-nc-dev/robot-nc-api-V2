import mongoose from "mongoose";

// Définition du schéma pour le fichier DBF
const AwFactureDetailsSchema = new mongoose.Schema(
  {
    NUMFACT: { type: String, maxlength: 7 }, // Numéro de facture
    NART: { type: String, maxlength: 6 }, // Code article
    DESIGN: { type: String, maxlength: 50 }, // Désignation
    QTE: { type: Number, default: 0 }, // Quantité (avec précision)
    PVTE: { type: Number, default: 0 }, // Prix de vente
    PREV: { type: Number, default: 0 }, // Prix prévu
    POURC: { type: Number, default: 0 }, // Pourcentage
    TYPFACT: { type: String, maxlength: 1 }, // Type de facture
    NONIMP: { type: String, maxlength: 1 }, // Non-imprimable
    DTVA: { type: Number, default: 0 }, // Taux de TVA
    POINTE: { type: String, maxlength: 1 }, // Pointe
    CLIENT: { type: String, maxlength: 4 }, // Code client
    PROMO: { type: String, maxlength: 1 }, // Promo (oui/non)
    STKREST: { type: Number, default: 0 }, // Stock restant
    NL: { type: Number, default: 0 }, // NL (nombre de lignes ?)
    COMPOSE: { type: String, maxlength: 2 }, // Composé (oui/non)
    PVTTC: { type: Number, default: 0 }, // Prix TTC
  },
  { timestamps: true } // Ajoute les champs `createdAt` et `updatedAt`
);

export default mongoose.model("AwFactureDetails", AwFactureDetailsSchema);
