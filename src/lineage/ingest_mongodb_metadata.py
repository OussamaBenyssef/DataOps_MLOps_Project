"""Script CLI pour lancer l'ingestion des métadonnées MongoDB vers DataHub."""

# Importe argparse pour gérer les arguments de ligne de commande.
import argparse
# Importe shlex pour afficher proprement la commande générée.
import shlex
# Importe subprocess pour exécuter la commande DataHub CLI.
import subprocess
# Importe sys pour retourner le code de sortie du processus.
import sys


# Définit la recette par défaut utilisée pour l'ingestion MongoDB.
DEFAULT_RECIPE_PATH = "/etc/datahub/recipes/mongodb_recipe.yml"


# Construit le parseur d'arguments CLI.
def build_parser() -> argparse.ArgumentParser:
    # Crée le parseur principal avec une description en français.
    parser = argparse.ArgumentParser(description="Lance l'ingestion MongoDB vers DataHub")
    # Ajoute l'argument du chemin de recette avec une valeur par défaut.
    parser.add_argument(
        "--recipe",
        default=DEFAULT_RECIPE_PATH,
        help="Chemin de la recette DataHub MongoDB (défaut: /etc/datahub/recipes/mongodb_recipe.yml)",
    )
    # Ajoute un mode aperçu pour valider la recette sans pousser les métadonnées.
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Active le mode aperçu DataHub pour vérifier la recette",
    )
    # Retourne le parseur configuré.
    return parser


# Construit la commande DataHub à exécuter en fonction des options utilisateur.
def build_command(recipe: str, preview: bool) -> list[str]:
    # Initialise la commande de base DataHub ingestion avec la recette fournie.
    command = ["datahub", "ingest", "-c", recipe]
    # Ajoute l'option de preview si demandée par l'utilisateur.
    if preview:
        # Ajoute le drapeau de preview officiel de la CLI DataHub.
        command.append("--preview")
    # Retourne la commande finale prête à l'exécution.
    return command


# Exécute la commande DataHub et retourne son code de sortie.
def run_ingestion(command: list[str]) -> int:
    # Affiche la commande exacte pour faciliter le debug opérationnel.
    print(f"Commande exécutée: {shlex.join(command)}")
    # Exécute la commande et attend la fin du processus fils.
    completed_process = subprocess.run(command, check=False)
    # Retourne le code de sortie pour propagation au shell appelant.
    return completed_process.returncode


# Point d'entrée principal du script.
def main() -> int:
    # Construit le parseur d'arguments.
    parser = build_parser()
    # Parse les arguments passés depuis la ligne de commande.
    args = parser.parse_args()
    # Construit la commande DataHub adaptée aux options fournies.
    command = build_command(recipe=args.recipe, preview=args.preview)
    # Lance l'ingestion et récupère le code retour de la CLI.
    return run_ingestion(command)


# Exécute le script uniquement s'il est lancé directement.
if __name__ == "__main__":
    # Quitte le programme en renvoyant le code de sortie de main().
    sys.exit(main())