Voici des exemples d'analyses correctes pour calibrer tes reponses.

<exemple>
<annonce>
- Description : Vente rapide cause mutation. T3 lumineux 62m2, 3eme etage avec ascenseur, double vitrage, parquet, cave. Copropriete de 24 lots, charges 85EUR/mois. Quartier calme proche tram.
- Prix : 135000 EUR
- Surface : 62 m2
- Nombre de pieces : 3
- Quartier : Centre-Ville
- DPE : C
- Adresse : Rue de la Prefecture, 25000 Besancon
- Charges copropriete : 85 EUR/mois
</annonce>
<analyse>
{
  "signaux_nego": ["vente rapide", "mutation"],
  "etat_bien": "bon_etat",
  "equipements": ["ascenseur", "double_vitrage", "parquet", "cave"],
  "red_flags": [],
  "info_copro": {"nb_lots": 24, "charges_annuelles_copro": 24480, "charges_annuelles_lot": 1020},
  "estimation_travaux": {
    "necessaire": false,
    "description": null,
    "budget_bas": null,
    "budget_haut": null
  },
  "scenarios_location": {
    "standard": {
      "loyer_nu": 580,
      "loyer_meuble": 650
    },
    "colocation": {
      "nb_chambres": 2,
      "loyer_par_chambre": 380,
      "loyer_total": 760
    }
  },
  "resume": "T3 en bon etat, centre-ville proche tram, vendeur presse (mutation). Cout total 135k EUR. En standard nu: renta ~5.2%, en colocation (2 chambres): ~6.8%. Apres negociation 10-15%, la colocation depasse 7.5%. Bon potentiel grace a la demande etudiante du quartier."
}
</analyse>
</exemple>

<exemple>
<annonce>
- Description : A saisir ! Appartement T2 refait a neuf, cuisine equipee, salle de bain moderne. Ideal investisseur, actuellement loue 480EUR/mois. Parking en sous-sol inclus. Immeuble bien entretenu.
- Prix : 89000 EUR
- Surface : 38 m2
- Nombre de pieces : 2
- Quartier : Battant
- DPE : D
- Adresse : Rue Battant, 25000 Besancon
- Charges copropriete : 60 EUR/mois
</annonce>
<analyse>
{
  "signaux_nego": ["a saisir"],
  "etat_bien": "tres_bon_etat",
  "equipements": ["cuisine_equipee", "parking"],
  "red_flags": ["DPE D - attention a la reglementation energetique future"],
  "info_copro": {"nb_lots": null, "charges_annuelles_copro": null, "charges_annuelles_lot": 720},
  "estimation_travaux": {
    "necessaire": false,
    "description": null,
    "budget_bas": null,
    "budget_haut": null
  },
  "scenarios_location": {
    "standard": {
      "loyer_nu": 480,
      "loyer_meuble": 540
    },
    "colocation": {
      "nb_chambres": null,
      "loyer_par_chambre": null,
      "loyer_total": null
    }
  },
  "resume": "T2 renove a Battant avec locataire en place (480 EUR/mois), renta brute 6.5% immediate. Colocation non applicable (T2, une seule chambre). Parking inclus est un atout. DPE D a surveiller pour conformite reglementaire. Prix/m2 de 2342 EUR dans la fourchette basse du quartier."
}
</analyse>
</exemple>

<exemple>
<annonce>
- Description : Appartement T3 a renover entierement. Travaux toiture votes en AG. Beaucoup de potentiel. Prix en consequence.
- Prix : 75000 EUR
- Surface : 58 m2
- Nombre de pieces : 3
- Quartier : Chablais
- DPE : Non renseigne
- Adresse : Avenue de Chardonnet, 25000 Besancon
- Charges copropriete : Non renseigne
</annonce>
<analyse>
{
  "signaux_nego": ["prix en consequence", "beaucoup de potentiel"],
  "etat_bien": "a_renover",
  "equipements": [],
  "red_flags": ["travaux toiture votes - appel de fonds probable", "DPE non renseigne - risque passoire energetique", "renovation complete necessaire - budget travaux a estimer"],
  "info_copro": {"nb_lots": null, "charges_annuelles_copro": null, "charges_annuelles_lot": null},
  "estimation_travaux": {
    "necessaire": true,
    "description": "Renovation complete (remise aux normes, redistribution probable). Travaux toiture copro en sus.",
    "budget_bas": 46400,
    "budget_haut": 69600
  },
  "scenarios_location": {
    "standard": {
      "loyer_nu": 530,
      "loyer_meuble": 600
    },
    "colocation": {
      "nb_chambres": 2,
      "loyer_par_chambre": 350,
      "loyer_total": 700
    }
  },
  "resume": "T3 a renover, prix bas (1293 EUR/m2) mais cout total estime 121k-145k EUR (prix + renovation 46-70k EUR) + appel de fonds toiture. En standard nu post-travaux: renta ~4.4-5.3%, en colocation: ~5.8-6.9%. Sans DPE ni info charges, risque eleve. Interessant uniquement si renovation maitrisee et negociation significative du prix."
}
</analyse>
</exemple>
