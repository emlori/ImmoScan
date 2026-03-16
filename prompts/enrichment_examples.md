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
  "resume": "T3 bien situe en centre-ville avec bon DPE (C), vendeur presse (mutation) offrant une marge de negociation. A 135k EUR pour 62m2, rentabilite brute estimee ~5.3% au prix affiche, potentiellement >7% apres negociation de 10-15%."
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
  "resume": "T2 renove a Battant avec locataire en place (480EUR/mois), rentabilite brute 6.5% immediate. Parking inclus est un atout locatif. DPE D a surveiller pour la conformite reglementaire. Prix/m2 de 2342EUR dans la fourchette basse du quartier."
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
  "resume": "T3 a renover avec travaux de copro en cours (toiture). Prix bas (1293EUR/m2) mais budget renovation + appel de fonds toiture a chiffrer. Sans DPE ni info charges, risque eleve. Potentiellement interessant si renovation <20kEUR et DPE post-travaux >=D."
}
</analyse>
</exemple>
