import mongoose from "mongoose";

// Définition du schéma pour le fichier DBF
const LeBroussardFactureSchema = new mongoose.Schema(
  {
    NUMFACT: { type: String}, // Numéro de facture
    TYPFACT: { type: String }, // Type de facture
    DATFACT: { type: Date }, // Date de la facture
    DATTRAV: { type: Date }, // Date du travail
    TIERS: { type: Number}, // Identifiant tiers
    GENER: { type: String }, // Génération
    DBCPT: { type: String }, // Débit/Crédit
    BONCDE: { type: String}, // Bon de commande
    REPRES: { type: Number }, // Représentant
    TEXTE: { type: String }, // Texte
    CHEQUE: { type: String }, // Numéro de chèque
    MONTANT: { type: Number }, // Montant
    FACTREM: { type: Number }, // Remise sur facture
    FACTNBLG: { type: Number }, // Nombre de lignes
    FACTREV: { type: Number }, // Revenu
    SUPPR: { type: String }, // Supprimé (oui/non)
    MONTAXES: { type: Number }, // Montant des taxes
    ACOMPTE: { type: String}, // Acompte
    STDEST: { type: Number}, // Stock destination
    STORI: { type: Number}, // Stock origine
    ETAT: { type: Number}, // État
    HEURE: { type: String }, // Heure
    AP: { type: String }, // AP
    EXTIERS: { type: Number }, // Tiers externe
    NOM: { type: String }, // Nom
  },
  { timestamps: true } // Ajoute les champs `createdAt` et `updatedAt`
);

export default mongoose.model("LeBroussardFacture", LeBroussardFactureSchema);
