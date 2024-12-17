import mongoose from "mongoose";

// Définition du schéma pour le fichier DBF
const AvbArticleSchema = new mongoose.Schema(
  {
    NART: { type: String }, // Code article
    DESIGN: { type: String }, // Désignation principale
    DESIGN2: { type: String }, // Désignation secondaire
    GENCOD: { type: String }, // Code barre
    REFER: { type: String }, // Référence
    FOURN: { type: Number, default: 0 }, // Fournisseur
    PREV: { type: Number, default: 0 }, // Prix prévu
    QT2: { type: Number, default: 0 }, // Quantité 2
    PR2: { type: Number, default: 0 }, // Prix 2
    QT3: { type: Number, default: 0 }, // Quantité 3
    PR3: { type: Number, default: 0 }, // Prix 3
    PVTE: { type: Number, default: 0 }, // Prix vente
    SMINI: { type: Number, default: 0 }, // Stock minimum
    STOCK: { type: Number, default: 0 }, // Stock actuel
    STLOC2: { type: Number, default: 0 }, // Stock local 2
    RESERV: { type: Number, default: 0 }, // Stock réservé
    UNITE: { type: String }, // Unité de mesure
    CONDITNM: { type: Number, default: 0 }, // Conditionnement
    ENCDE: { type: Number, default: 0 }, // En commande
    GROUPE: { type: String }, // Groupe
    PDETAIL: { type: Number, default: 0 }, // Prix détail
    PACHAT: { type: Number, default: 0 }, // Prix d'achat
    TAXES: { type: Number, default: 0 }, // Taxes
    OBSERV: { type: String }, // Observations
    POURC: { type: Number, default: 0 }, // Pourcentage
    DERPREV: { type: Number, default: 0 }, // Dernier prix prévu
    V1: { type: Number, default: 0 }, // Ventes mois 1
    V2: { type: Number, default: 0 }, // Ventes mois 2
    V3: { type: Number, default: 0 }, // Ventes mois 3
    V4: { type: Number, default: 0 }, // Ventes mois 4
    V5: { type: Number, default: 0 }, // Ventes mois 5
    V6: { type: Number, default: 0 }, // Ventes mois 6
    V7: { type: Number, default: 0 }, // Ventes mois 7
    V8: { type: Number, default: 0 }, // Ventes mois 8
    V9: { type: Number, default: 0 }, // Ventes mois 9
    V10: { type: Number, default: 0 }, // Ventes mois 10
    V11: { type: Number, default: 0 }, // Ventes mois 11
    V12: { type: Number, default: 0 }, // Ventes mois 12
    DOUANE: { type: String }, // Code douane
    DEVISE: { type: String }, // Devise
    DATINV: { type: Date }, // Date inventaire
    DEPREC: { type: Number, default: 0 }, // Dépréciation
    CODMAJ: { type: String }, // Code majeur
    PVPROMO: { type: Number, default: 0 }, // Prix promo
    ATVA: { type: Number, default: 0 }, // Taux TVA
    CODTAR: { type: String }, // Code tarifaire
    DPROMOD: { type: Date }, // Date promo début
    DPROMOF: { type: Date }, // Date promo fin
    DATINV2: { type: Date }, // Deuxième date inventaire
    S1: { type: Number, default: 0 }, // Stock 1
    S2: { type: Number, default: 0 }, // Stock 2
    S3: { type: Number, default: 0 }, // Stock 3
    S4: { type: Number, default: 0 }, // Stock 4
    S5: { type: Number, default: 0 }, // Stock 5
    GISM1: { type: String }, // Groupe magasin 1
    GISM2: { type: String }, // Groupe magasin 2
    GISM3: { type: String }, // Groupe magasin 3
    GISM4: { type: String }, // Groupe magasin 4
    GISM5: { type: String }, // Groupe magasin 5
    PLACE: { type: String }, // Emplacement
    TARIFL: { type: Boolean, default: false }, // Tarif local
    RUP1: { type: Number, default: 0 }, // Rupture mois 1
    RUP2: { type: Number, default: 0 }, // Rupture mois 2
    RUP3: { type: Number, default: 0 }, // Rupture mois 3
    RUP4: { type: Number, default: 0 }, // Rupture mois 4
    RUP5: { type: Number, default: 0 }, // Rupture mois 5
    RUP6: { type: Number, default: 0 }, // Rupture mois 6
    RUP7: { type: Number, default: 0 }, // Rupture mois 7
    RUP8: { type: Number, default: 0 }, // Rupture mois 8
    RUP9: { type: Number, default: 0 }, // Rupture mois 9
    RUP10: { type: Number, default: 0 }, // Rupture mois 10
    RUP11: { type: Number, default: 0 }, // Rupture mois 11
    RUP12: { type: Number, default: 0 }, // Rupture mois 12
    TEXTE: { type: String }, // Texte
    GENDOUBL: { type: String }, // Générique doublé
    ASSOCIE: { type: String }, // Associé
    FOTO: { type: String }, // Photo
    WEB: { type: String }, // Web
    DESIFRN: { type: String }, // Désignation FR
    COULR: { type: String }, // Couleur
    CDESPEC: { type: Number, default: 0 }, // Code spécifique
    STSECUR: { type: Number, default: 0 }, // Stock sécurité
    RENV: { type: String }, // Renvoi
    COMPOSE: { type: String }, // Composé
    CODTGC: { type: String }, // Code TGC
    CREATION: { type: Date }, // Date de création
    VOL: { type: Number, default: 0 }, // Volume
    KL: { type: String }, // KL
    PVTETTC: { type: Number, default: 0 }, // Prix TTC
    TXADEDUIRE: { type: Number, default: 0 }, // Taux à déduire
  },
  { timestamps: true } // Ajoute les champs `createdAt` et `updatedAt`
);

export default mongoose.model("AvbArticle", AvbArticleSchema);
