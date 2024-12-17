import mongoose from 'mongoose';

// Définition du schéma pour le fichier DBF
const MeareFournisseurSchema = new mongoose.Schema(
  {
    NOM: { type: String},
    AD1: { type: String },
    AD2: { type: String },
    AD3: { type: String },
    AD4: { type: String },
    TEL: { type: String},
    TLX: { type: String},
    FOURN: { type: Number}, // Nombre avec 3 chiffres max
    FAX: { type: String },
    OBSERV: { type: String},
    DELAPRO: { type: Number }, // Nombre avec 3 chiffres max
    COEFSMINI: { type: Number}, // Nombre avec 4 chiffres et 2 décimales
    TEXTE: { type: String},
    ENT1: { type: String },
    ENT2: { type: String },
    ENT3: { type: String },
    NOT1: { type: String },
    NOT2: { type: String },
    NOT3: { type: String },
    NOT4: { type: String },
    NOT5: { type: String },
    NOT6: { type: String },
    NOT7: { type: String },
    NOT8: { type: String },
    NOT9: { type: String },
    NOT10: { type: String },
    FRANCO: { type: Number }, // Nombre avec 8 chiffres max
    AD5: { type: String},
    LOCAL: { type: String },
  },
  { timestamps: true } // Ajoute les champs `createdAt` et `updatedAt`
);

// Export du modèle
export default mongoose.model('MeareFournisseur', MeareFournisseurSchema);
