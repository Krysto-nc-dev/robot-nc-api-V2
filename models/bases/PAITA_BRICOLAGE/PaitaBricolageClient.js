import mongoose from "mongoose";

// Définition du schéma pour le fichier DBF FOURNISS
const PaitaBricolageClientSchema = new mongoose.Schema(
  {
    NOM: { type: String, unique: false }, // Nom
    AD1: { type: String }, // Adresse 1
    AD2: { type: String }, // Adresse 2
    AD3: { type: String }, // Adresse 3
    AD4: { type: String }, // Adresse 4
    AD5: { type: String }, // Adresse 5
    TEL: { type: String}, // Téléphone
    TIERS: { type: Number}, // Code tiers
    REMISE: { type: Number }, // Remise
    NBEXFACT: { type: Number }, // Nombre exemplaire facture
    REPRES: { type: Number}, // Représentant
    OBSERV: { type: String }, // Observation
    PROFES: { type: String }, // Profession
    DEBIT: { type: Number}, // Débit
    CREDIT: { type: Number}, // Crédit
    CATCLI: { type: String }, // Catégorie client
    DEBIMAX: { type: Number }, // Débit maximum
    FAX: { type: String}, // Fax
    INTERLOC: { type: String }, // Interlocuteur
    BANQUE: { type: String }, // Banque
    TEXTE: { type: String }, // Texte
    CODREM: { type: String }, // Code remise
    GROUPE: { type: String}, // Groupe
    TARIFL: { type: Boolean }, // Tarif L
    CLTVA: { type: String }, // Code TVA
    CODTARIF: { type: String }, // Code tarif
    TARIF: { type: String}, // Tarif
    CONDPAI: { type: String}, // Condition de paiement
    CREATION: { type: Date }, // Date de création
    LOGIN: { type: String }, // Login utilisateur
    INTPASS: { type: String }, // Mot de passe interne
    ADMAIL: { type: String }, // Adresse mail
    ECOTAXE: { type: String}, // Ecotaxe
    TYPE: { type: String }, // Type
    CATEGORIE: { type: String }, // Catégorie
    CONTACT1: { type: String }, // Contact 1
    CONTACT2: { type: String }, // Contact 2
    CONTACT3: { type: String }, // Contact 3
    CONTACT4: { type: String }, // Contact 4
    ECHEANCE2: { type: Number }, // Échéance 2
    FDM: { type: String }, // FDM
    OBSERVAT: { type: String }, // Observations
    CAUTION: { type: String}, // Caution
    DATCAUTION: { type: Date }, // Date de caution
    BLOCAGE: { type: String}, // Blocage
    DATBLOCAGE: { type: Date }, // Date de blocage
    FACTURE: { type: String}, // Facture
    DATFACTURE: { type: Date }, // Date de facture
    RELANCE: { type: String}, // Relance
    FAITPAR: { type: String }, // Fait par
    DATFAIPAR: { type: Date }, // Date fait par
    ACTION: { type: String}, // Action
    ACTPAR: { type: String }, // Action par
    DATACTPAR: { type: Date }, // Date action par
    COMMENT: { type: String }, // Commentaire
    COMMENTPAR: { type: String}, // Commentaire par
    DATCOMPAR: { type: Date }, // Date commentaire par
    MEMOLIVR1: { type: String}, // Mémo livraison 1
    MEMOLIVR2: { type: String}, // Mémo livraison 2
    MEMOLIVR3: { type: String }, // Mémo livraison 3
    BALANCE: { type: String}, // Balance
    DATECHEAN: { type: String }, // Date échéance
  },
  { timestamps: true } // Ajoute les champs `createdAt` et `updatedAt`
);

export default mongoose.model("PaitaBricolageClient", PaitaBricolageClientSchema);
