import pandas as pd

# --- CONFIGURATION ---
FICHIER_SOURCE = 'transaction.csv'
FICHIER_SORTIE = 'transaction_sample_by_customer.csv'
# Fraction d'échantillonnage : 0.20 correspond à 1 client sur 5
FRACTION_ECHANTILLON = 0.20 
ENCODAGE_SOURCE = 'latin1'
ENCODAGE_CIBLE = 'utf-8-sig'

def sample_by_customer_id(source_path, output_path, fraction, source_encoding, target_encoding):
    """
    Charge un fichier CSV, extrait un échantillon aléatoire de customer_id
    et sauvegarde toutes les lignes de ces clients dans un nouveau fichier.
    """
    print(f"--- Démarrage de l'échantillonnage par customer_id pour le fichier : {source_path} ---")

    try:
        # Étape 1: Chargement du fichier source
        print(f"1. Chargement du fichier source avec l'encodage '{source_encoding}'...")
        df = pd.read_csv(source_path, encoding=source_encoding)
        print(f"-> Fichier chargé. Nombre total de lignes : {len(df)}")

        # Étape 2: Lister les clients uniques
        unique_customers = df['customer_id'].unique()
        n_unique_customers = len(unique_customers)
        print(f"-> {n_unique_customers} clients uniques trouvés.")

        # Étape 3: Échantillonner les customer_id
        n_customers_to_sample = int(n_unique_customers * fraction)
        print(f"3. Tirage au sort de {n_customers_to_sample} clients uniques ({fraction*100:.0f}% du total)...")
        
        # Convertir en Series pour utiliser la méthode .sample() de pandas
        sampled_customer_ids = pd.Series(unique_customers).sample(
            n=n_customers_to_sample,
            random_state=42  # random_state pour garantir que le tirage au sort soit toujours le même
        )
        print("-> Clients tirés au sort avec succès.")

        # Étape 4: Filtrer le DataFrame original pour ne garder que les lignes des clients sélectionnés
        print("4. Création du DataFrame échantillonné...")
        df_sampled = df[df['customer_id'].isin(sampled_customer_ids)]
        print(f"-> Le nouvel échantillon contient {len(df_sampled)} lignes pour {len(sampled_customer_ids)} clients.")

        # Étape 5: Sauvegarde du nouvel échantillon en UTF-8
        print(f"5. Sauvegarde de l'échantillon dans '{output_path}' avec l'encodage '{target_encoding}'...")
        df_sampled.to_csv(output_path, index=False, encoding=target_encoding)
        
        print("\n--- Opération terminée avec succès ! ---")
        print(f"Le fichier '{output_path}' a été créé.")

    except FileNotFoundError:
        print(f"\nERREUR : Le fichier source '{source_path}' est introuvable.")
    except KeyError:
        print(f"\nERREUR : La colonne 'customer_id' est introuvable dans le fichier.")
    except Exception as e:
        print(f"\nUne erreur inattendue est survenue : {e}")

# Point d'entrée du script
if __name__ == "__main__":
    sample_by_customer_id(
        source_path=FICHIER_SOURCE,
        output_path=FICHIER_SORTIE,
        fraction=FRACTION_ECHANTILLON,
        source_encoding=ENCODAGE_SOURCE,
        target_encoding=ENCODAGE_CIBLE
    )