"""Tests unitaires du script d'ingestion MongoDB vers DataHub."""

# Importe le module à tester.
from src.lineage.ingest_mongodb_metadata import build_command


# Vérifie la commande sans mode aperçu.
def test_build_command_without_preview():
    # Définit un chemin de recette fictif pour le test.
    recipe_path = "/tmp/mongodb_recipe.yml"
    # Construit la commande sans preview.
    command = build_command(recipe=recipe_path, preview=False)
    # Vérifie la structure exacte de la commande retournée.
    assert command == ["datahub", "ingest", "-c", recipe_path]


# Vérifie la commande avec mode aperçu.
def test_build_command_with_preview():
    # Définit un chemin de recette fictif pour le test.
    recipe_path = "/tmp/mongodb_recipe.yml"
    # Construit la commande avec preview activé.
    command = build_command(recipe=recipe_path, preview=True)
    # Vérifie la présence du drapeau --preview en fin de commande.
    assert command == ["datahub", "ingest", "-c", recipe_path, "--preview"]
