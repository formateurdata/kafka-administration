#!/usr/bin/env python3
# ------------------------------------------------------------------
# Generation des notebooks des TP et des demos a partir de sources
# uniques au format "percent" (cellules separees par "# %%").
#
#   tps/jourN/tpM_*/notebook.source.py  ->  enonce.ipynb et corrige.ipynb
#   demos-tp/demo-tpM.source.py         ->  demo-tpM.ipynb
#
# Marqueurs de cellule :
#   # %%                       code, dans tous les notebooks
#   # %% [markdown]            texte, dans tous les notebooks
#   # %% enonce                code ou texte de l'enonce seulement (zones TODO)
#   # %% corrige               code ou texte du corrige seulement (solutions)
#   # %% [markdown] corrige    idem pour une cellule de texte
# Les lignes d'une cellule de texte commencent par "# " (ou "#" seul).
# Dans le texte, @@DATE_EXECUTION@@ est remplace par la date d'execution.
#
# Usage, depuis la racine de la formation :
#   generer seulement (sur l'hote) :
#     uv run --with nbformat lab/jupyter/build_notebooks.py [tp1 demo-tp1 ...]
#   generer puis executer corriges et demos sur le lab (sorties reelles) :
#     docker exec -w "$PWD" jupyter python lab/jupyter/build_notebooks.py --executer [tp1 demo-tp1 ...]
#   executer aussi l'enonce, TODO vides : seules les verifications doivent echouer
#   (sorties non enregistrees ; le lab doit etre dans l'etat attendu au debut du TP) :
#     docker exec -w "$PWD" jupyter python lab/jupyter/build_notebooks.py --tester-enonce tp1
#   rejouer des corriges sans rien ecrire, pour amener le lab a l'etat de debut d'un TP :
#     docker exec -w "$PWD" jupyter python lab/jupyter/build_notebooks.py --rejouer tp1 tp2
#   reporter une correction de texte en gardant les sorties deja enregistrees
#   (refuse si une cellule de code a change : il faut alors reexecuter) :
#     uv run --with nbformat lab/jupyter/build_notebooks.py --textes tp2
# Sans cible : toutes les sources trouvees.
# ------------------------------------------------------------------
import argparse
import datetime
import glob
import os
import re
import sys

import nbformat

ICI = os.path.dirname(os.path.abspath(__file__))
RACINE = os.path.dirname(os.path.dirname(ICI))
MARQUEUR = re.compile(r"^# %%(?P<md> \[markdown\])?(?: (?P<tag>enonce|corrige))?\s*$")
ERREURS = []


def sources():
    """Associe chaque cible (tp1, demo-tp1...) a son fichier source."""
    cibles = {}
    for f in sorted(glob.glob(os.path.join(RACINE, "tps", "jour*", "tp*", "notebook.source.py"))):
        nom = os.path.basename(os.path.dirname(f)).split("_")[0]
        cibles[nom] = f
    for f in sorted(glob.glob(os.path.join(RACINE, "demos-tp", "demo-tp*.source.py"))):
        cibles[os.path.basename(f)[:-len(".source.py")]] = f
    return cibles


def lire_cellules(chemin):
    """Decoupe une source en cellules : liste de (type, tag, texte)."""
    cellules, courante = [], None
    with open(chemin, encoding="utf-8") as fh:
        for num, ligne in enumerate(fh.read().split("\n"), 1):
            m = MARQUEUR.match(ligne)
            if m:
                courante = ["markdown" if m.group("md") else "code", m.group("tag"), [], num]
                cellules.append(courante)
            elif ligne.startswith("# %%"):
                ERREURS.append("%s:%d marqueur inconnu : %s" % (chemin, num, ligne))
            elif courante is not None:
                courante[2].append(ligne)
    resultat = []
    for type_, tag, lignes, num in cellules:
        if type_ == "markdown":
            texte = []
            for ligne in lignes:
                if ligne.startswith("# "):
                    texte.append(ligne[2:])
                elif ligne in ("#", ""):
                    texte.append("")
                else:
                    ERREURS.append("%s:%d ligne de texte sans '# ' : %s" % (chemin, num, ligne))
            lignes = texte
        texte = "\n".join(lignes).strip("\n")
        # Une cellule avec commande shell (!cmd) ou magique (%%bash) n'est pas du Python pur : non compilee.
        shell = any(l.lstrip().startswith(("!", "%")) or "= !" in l for l in lignes)
        if texte and type_ == "code" and not shell:
            try:
                compile(texte, "%s:%d" % (chemin, num), "exec")
            except SyntaxError as e:
                ERREURS.append("%s:%d cellule de code invalide : %s" % (chemin, num, e))
        if texte:
            resultat.append((type_, tag, texte))
    return resultat


def construire(cellules, garder, nom):
    """Notebook avec les cellules sans tag et celles du tag garde."""
    nb = nbformat.v4.new_notebook()
    nb.metadata = {
        "kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
        "language_info": {"name": "python"},
    }
    for i, (type_, tag, texte) in enumerate(cellules, 1):
        if tag not in (None, garder):
            continue
        if type_ == "markdown":
            c = nbformat.v4.new_markdown_cell(texte)
        else:
            c = nbformat.v4.new_code_cell(texte)
            if tag == "enonce":
                c.metadata["tags"] = ["todo"]
        # Identifiants stables : un notebook regenere ne change que la ou la source change.
        c.id = "%s-%03d" % (nom, i)
        nb.cells.append(c)
    return nb


def dater(nb, date):
    for c in nb.cells:
        if c.cell_type == "markdown":
            c.source = c.source.replace("@@DATE_EXECUTION@@", date)


def executer(nb, dossier, erreurs_permises=False):
    from nbclient import NotebookClient

    NotebookClient(nb, timeout=900, kernel_name="python3", allow_errors=erreurs_permises,
                   resources={"metadata": {"path": dossier}}).execute()


def tester_enonce(nb, dossier, nom):
    """Execute l'enonce sans le modifier : seules les verifications peuvent echouer."""
    executer(nb, dossier, erreurs_permises=True)
    for c in nb.cells:
        erreurs = [o for o in c.get("outputs", []) if o.output_type == "error"]
        if not erreurs:
            continue
        verification = c.source.startswith("# Verification")
        print("  %s %s -> %s" % ("verification" if verification else "ERREUR", c.source.splitlines()[0][:60],
                                 erreurs[0].ename))
        if not verification:
            ERREURS.append("%s : erreur hors verification (%s)" % (nom, c.source.splitlines()[0][:60]))


def reprendre_sorties(nb, chemin, nom):
    """Reprend les sorties du notebook deja execute ; renvoie sa date d'execution, ou None si le code a change."""
    if not os.path.exists(chemin):
        ERREURS.append("%s : %s absent, executer d'abord (--executer)" % (nom, os.path.relpath(chemin, RACINE)))
        return None
    ancien = nbformat.read(chemin, as_version=4)
    avant = [c for c in ancien.cells if c.cell_type == "code"]
    apres = [c for c in nb.cells if c.cell_type == "code"]
    if [c.source for c in avant] != [c.source for c in apres]:
        ERREURS.append("%s : code modifie depuis l'execution, %s non reecrit : reexecuter (--executer)"
                       % (nom, os.path.relpath(chemin, RACINE)))
        return None
    for nouvelle, ancienne in zip(apres, avant):
        nouvelle.outputs, nouvelle.execution_count = ancienne.outputs, ancienne.execution_count
        if "execution" in ancienne.metadata:
            nouvelle.metadata["execution"] = ancienne.metadata["execution"]
    nb.metadata = ancien.metadata
    date = re.search(r"\d\d/\d\d/\d{4} \d\d:\d\d UTC",
                     "\n".join(c.source for c in ancien.cells if c.cell_type == "markdown"))
    return date.group(0) if date else "jj/mm/aaaa"


def ecrire(nb, chemin):
    nbformat.validate(nb)
    with open(chemin, "w", encoding="utf-8") as fh:
        nbformat.write(nb, fh)
    print("ecrit : %s (%d cellules)" % (os.path.relpath(chemin, RACINE), len(nb.cells)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cibles", nargs="*", help="tp1 ... tp8, demo-tp1 ... (defaut : toutes)")
    ap.add_argument("--executer", action="store_true",
                    help="executer corriges et demos sur le lab (dans le conteneur jupyter)")
    ap.add_argument("--tester-enonce", action="store_true",
                    help="executer aussi les enonces, sans enregistrer leurs sorties")
    ap.add_argument("--rejouer", action="store_true",
                    help="executer les corriges sans rien ecrire (etat du lab pour le TP suivant)")
    ap.add_argument("--textes", action="store_true",
                    help="regenerer en gardant les sorties enregistrees (code inchange seulement)")
    args = ap.parse_args()
    if args.textes and (args.executer or args.tester_enonce or args.rejouer):
        sys.exit("--textes ne se combine pas avec --executer, --tester-enonce ni --rejouer.")
    toutes = sources()
    cibles = args.cibles or list(toutes)
    inconnues = [c for c in cibles if c not in toutes]
    if inconnues:
        sys.exit("Cibles inconnues : %s (connues : %s)" % (", ".join(inconnues), ", ".join(toutes)))
    if (args.executer or args.tester_enonce or args.rejouer) and not os.path.exists("/var/run/docker.sock"):
        sys.exit("--executer, --tester-enonce et --rejouer se lancent dans le conteneur jupyter "
                 "(voir l'en-tete du script).")
    if args.rejouer:
        for nom in cibles:
            if nom.startswith("demo"):
                continue
            print("rejeu : %s" % nom)
            executer(construire(lire_cellules(toutes[nom]), "corrige", nom), os.path.dirname(toutes[nom]))
        return

    # Sans execution, la date reste a remplir : jj/mm/aaaa.
    # Heure UTC explicite : le conteneur est en UTC, l'hote a l'heure locale.
    maintenant = datetime.datetime.now(datetime.timezone.utc)
    date = maintenant.strftime("%d/%m/%Y %H:%M UTC") if args.executer else "jj/mm/aaaa"
    for nom in cibles:
        src = toutes[nom]
        dossier = os.path.dirname(src)
        cellules = lire_cellules(src)
        if nom.startswith("demo"):
            sorties = [("demo", nom + ".ipynb", True)]
        else:
            sorties = [("enonce", "enonce.ipynb", False), ("corrige", "corrige.ipynb", True)]
        for garder, fichier, a_executer in sorties:
            nb = construire(cellules, garder, nom)
            if garder == "enonce" and args.tester_enonce:
                print("test de l'enonce : %s" % nom)
                tester_enonce(construire(cellules, garder, nom), dossier, nom)
            if args.tester_enonce and not args.executer:
                # Test seul : aucun fichier reecrit, le corrige garde ses sorties.
                continue
            if a_executer and args.executer:
                print("execution : %s/%s" % (nom, fichier))
                executer(nb, dossier)
            date_sorties = date
            if a_executer and args.textes:
                date_sorties = reprendre_sorties(nb, os.path.join(dossier, fichier), nom)
                if date_sorties is None:
                    continue
            dater(nb, date_sorties if a_executer else "")
            ecrire(nb, os.path.join(dossier, fichier))
    for e in ERREURS:
        print("ATTENTION " + e, file=sys.stderr)
    sys.exit(1 if ERREURS else 0)


if __name__ == "__main__":
    main()
