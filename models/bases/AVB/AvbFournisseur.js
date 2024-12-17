import mongoose from 'mongoose';

// Définition du schéma pour le fichier DBF
const AvbFournisseurSchema = new mongoose.Schema(
  {
    NOM: { type: String, maxlength: 30},
    AD1: { type: String, maxlength: 30 },
    AD2: { type: String, maxlength: 30 },
    AD3: { type: String, maxlength: 30 },
    AD4: { type: String, maxlength: 30 },
    TEL: { type: String, maxlength: 14 },
    TLX: { type: String, maxlength: 50 },
    FOURN: { type: Number, min: 0, max: 999 }, // Nombre avec 3 chiffres max
    FAX: { type: String, maxlength: 15 },
    OBSERV: { type: String, maxlength: 40 },
    DELAPRO: { type: Number, min: 0, max: 999 }, // Nombre avec 3 chiffres max
    COEFSMINI: { type: Number, min: 0, max: 9999.99 }, // Nombre avec 4 chiffres et 2 décimales
    TEXTE: { type: String, maxlength: 1 },
    ENT1: { type: String, maxlength: 78 },
    ENT2: { type: String, maxlength: 78 },
    ENT3: { type: String, maxlength: 78 },
    NOT1: { type: String, maxlength: 78 },
    NOT2: { type: String, maxlength: 78 },
    NOT3: { type: String, maxlength: 78 },
    NOT4: { type: String, maxlength: 78 },
    NOT5: { type: String, maxlength: 78 },
    NOT6: { type: String, maxlength: 78 },
    NOT7: { type: String, maxlength: 78 },
    NOT8: { type: String, maxlength: 78 },
    NOT9: { type: String, maxlength: 78 },
    NOT10: { type: String, maxlength: 78 },
    FRANCO: { type: Number, min: 0, max: 99999999 }, // Nombre avec 8 chiffres max
    AD5: { type: String, maxlength: 30 },
    LOCAL: { type: String, maxlength: 1 },
  },
  { timestamps: true } // Ajoute les champs `createdAt` et `updatedAt`
);

// Export du modèle
export default mongoose.model('AvbFournisseur', AvbFournisseurSchema);
