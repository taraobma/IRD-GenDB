import io
import csv
import json
import os
from flask import Flask, render_template, request, Response, jsonify
import mariadb

app = Flask(__name__)

# Load ClinVar cache once at startup (sits next to app.py)
_clinvar_path = os.path.join(os.path.dirname(__file__), "clinvar_cache.json")
try:
    with open(_clinvar_path) as _f:
        CLINVAR = json.load(_f)
except FileNotFoundError:
    CLINVAR = {}

# ── DB helpers ────────────────────────────────────────────────────────────────

def get_conn():
    return mariadb.connect(
        host="bioed-new.bu.edu",
        database="Team8",
        user="zona7721",
        password="zona7721",
        port=4253
    )

def query(sql, params=()):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    conn.close()
    return cols, rows

# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.route("/")

# Dashboard page with summary statistics and charts
@app.route("/dashboard")
def dashboard():
    _, r = query("SELECT COUNT(*) FROM patient_table")
    total_patients = r[0][0]

    _, r = query("SELECT COUNT(*) FROM variant_table")
    total_variants = r[0][0]

    # Count distinct genes in variant_table
    _, r = query("SELECT COUNT(DISTINCT gene) FROM variant_table")
    total_genes = r[0][0]

    return render_template("dashboard.html",
                           total_patients=total_patients,
                           total_variants=total_variants,
                           total_genes=total_genes)
                           
@app.route("/about")
def about():
    return render_template("about.html")

# Help page with instructions on how to use the dashboard and interpret the data, as well as info about the data source and limitations
@app.route("/help")
def help():
    return render_template("help.html")


@app.route("/api/chart-data")
def chart_data():
    # ── Diagnosis (RP*→RP, MD*→MD) ───────────────────────────────────────────
    _, rows = query("""
        SELECT diagnosis, COUNT(*) AS cnt
        FROM patient_table
        WHERE diagnosis IS NOT NULL AND diagnosis != ''
        GROUP BY diagnosis ORDER BY cnt DESC
    """)
    _dgrp = {}
    for diag, cnt in rows:
        key = 'RP' if (diag and diag.startswith('RP')) else \
              'MD' if (diag and diag.startswith('MD')) else diag
        _dgrp[key] = _dgrp.get(key, 0) + cnt
    sorted_diag = sorted(_dgrp.items(), key=lambda x: -x[1])
    diagnoses   = [x[0] for x in sorted_diag]
    diag_counts = [x[1] for x in sorted_diag]
    diag_total  = sum(diag_counts) or 1
    diag_pcts   = [round(c / diag_total * 100, 1) for c in diag_counts]

    # ── Top 10 genes ─────────────────────────────────────────────────────────
    _, rows = query("""
        SELECT v.gene, COUNT(DISTINCT p.sample_id) AS cnt
        FROM variant_table v
        JOIN patient_table p ON p.variant1_id = v.variant_id
                             OR p.variant2_id = v.variant_id
        WHERE v.gene IS NOT NULL AND v.gene != ''
        GROUP BY v.gene ORDER BY cnt DESC LIMIT 10
    """)
    genes       = [r[0] for r in rows]
    gene_counts = [r[1] for r in rows]

    # Gene × diagnosis class (RP*→RP, MD*→MD)
    gene_diag_classes = []
    gene_diag_matrix  = {}
    if genes:
        ph = ','.join(['?'] * len(genes))
        _, gd_rows = query(f"""
            SELECT v.gene, p.diagnosis, COUNT(DISTINCT p.sample_id) AS cnt
            FROM variant_table v
            JOIN patient_table p ON p.variant1_id = v.variant_id
                                 OR p.variant2_id = v.variant_id
            WHERE v.gene IN ({ph})
              AND p.diagnosis IS NOT NULL AND p.diagnosis != ''
            GROUP BY v.gene, p.diagnosis
        """, genes)

        def _dcls(d):
            if d and d.startswith('RP'): return 'RP'
            if d and d.startswith('MD'): return 'MD'
            return d or 'Other'

        raw = {}
        for gene, diag, cnt in gd_rows:
            cls = _dcls(diag)
            raw.setdefault(cls, {}).setdefault(gene, 0)
            raw[cls][gene] += cnt

        ordered = []
        if 'RP' in raw: ordered.append('RP')
        if 'MD' in raw: ordered.append('MD')
        ordered += sorted(c for c in raw if c not in ('RP', 'MD'))
        gene_diag_classes = ordered
        gene_diag_matrix  = {cls: [raw[cls].get(g, 0) for g in genes] for cls in ordered}

    # ── Variant types ─────────────────────────────────────────────────────────
    _, rows = query("""
        SELECT variant_change_type, COUNT(*) AS cnt
        FROM variant_table
        WHERE variant_change_type IS NOT NULL AND variant_change_type != ''
        GROUP BY variant_change_type ORDER BY cnt DESC
    """)
    vtypes       = [r[0] for r in rows]
    vtype_counts = [r[1] for r in rows]

    # ── Inheritance (AR*→AR, AD*→AD, XL*→XL) + Not Found ────────────────────
    _, rows = query("""
        SELECT inheritancepattern, COUNT(*) AS cnt
        FROM patient_table
        WHERE inheritancepattern IS NOT NULL AND inheritancepattern != ''
        GROUP BY inheritancepattern ORDER BY cnt DESC
    """)
    _igrp = {}
    for pat, cnt in rows:
        key = 'AR' if (pat and pat.startswith('AR')) else \
              'AD' if (pat and pat.startswith('AD')) else \
              'XL' if (pat and pat.startswith('XL')) else pat or 'Other'
        _igrp[key] = _igrp.get(key, 0) + cnt

    _, nf = query("""
        SELECT COUNT(*) FROM patient_table
        WHERE inheritancepattern IS NULL OR inheritancepattern = ''
    """)
    not_found_cnt = nf[0][0]

    sorted_inh = sorted(_igrp.items(), key=lambda x: -x[1])
    if not_found_cnt:
        sorted_inh.append(('Not Found', not_found_cnt))

    inh_total  = sum(v for _, v in sorted_inh) or 1
    inh        = [x[0] for x in sorted_inh]
    inh_counts = [x[1] for x in sorted_inh]
    inh_pcts   = [round(c / inh_total * 100, 1) for c in inh_counts]

    return jsonify(
        diagnoses=diagnoses, diag_counts=diag_counts, diag_pcts=diag_pcts,
        genes=genes, gene_counts=gene_counts,
        gene_diag_classes=gene_diag_classes, gene_diag_matrix=gene_diag_matrix,
        vtypes=vtypes, vtype_counts=vtype_counts,
        inh=inh, inh_counts=inh_counts, inh_pcts=inh_pcts
    )

@app.route("/api/hotspot")
def hotspot():
    diag = request.args.get("diag", "").strip()
    gene = request.args.get("gene", "").strip()

    if diag:
        if diag == 'RP':
            where, params = "p.diagnosis LIKE 'RP%'", ()
        elif diag == 'MD':
            where, params = "p.diagnosis LIKE 'MD%'", ()
        else:
            where, params = "p.diagnosis = ?", (diag,)

        _, rows = query(f"""
            SELECT v.gene, v.hgvsc, v.protein_change,
                   v.variant_change_type, v.algorithm_prediction,
                   v.taiwan_biobank_af, v.gnomad_exome_east_af,
                   COUNT(DISTINCT p.sample_id) AS cnt
            FROM variant_table v
            JOIN patient_table p ON p.variant1_id = v.variant_id
                                 OR p.variant2_id = v.variant_id
            WHERE {where}
            GROUP BY v.variant_id, v.gene, v.hgvsc, v.protein_change,
                     v.variant_change_type, v.algorithm_prediction,
                     v.taiwan_biobank_af, v.gnomad_exome_east_af
            ORDER BY cnt DESC LIMIT 10
        """, params)
        _, tot = query(f"""
            SELECT COUNT(DISTINCT sample_id) FROM patient_table p WHERE {where}
        """, params)

    elif gene:
        params = (gene,)
        _, rows = query("""
            SELECT v.gene, v.hgvsc, v.protein_change,
                   v.variant_change_type, v.algorithm_prediction,
                   v.taiwan_biobank_af, v.gnomad_exome_east_af,
                   COUNT(DISTINCT p.sample_id) AS cnt
            FROM variant_table v
            JOIN patient_table p ON p.variant1_id = v.variant_id
                                 OR p.variant2_id = v.variant_id
            WHERE v.gene = ?
            GROUP BY v.variant_id, v.gene, v.hgvsc, v.protein_change,
                     v.variant_change_type, v.algorithm_prediction,
                     v.taiwan_biobank_af, v.gnomad_exome_east_af
            ORDER BY cnt DESC LIMIT 10
        """, params)
        _, tot = query("""
            SELECT COUNT(DISTINCT p.sample_id)
            FROM variant_table v
            JOIN patient_table p ON p.variant1_id = v.variant_id
                                 OR p.variant2_id = v.variant_id
            WHERE v.gene = ?
        """, params)

    else:
        return jsonify([])

    total = tot[0][0] or 1
    return jsonify([{
        'gene': r[0], 'hgvsc': r[1] or '—', 'protein_change': r[2] or '—',
        'variant_type': r[3] or '—', 'pathogenicity': r[4] or '—',
        'taiwan_af': r[5] or '—', 'gnomad_af': r[6] or '—',
        'count': r[7], 'pct': round(r[7] / total * 100, 1)
    } for r in rows])

@app.route("/api/genes")
def gene_list():
    _, rows = query("""
        SELECT v.gene, COUNT(DISTINCT p.sample_id) AS cnt
        FROM variant_table v
        JOIN patient_table p ON p.variant1_id = v.variant_id
                             OR p.variant2_id = v.variant_id
        WHERE v.gene IS NOT NULL AND v.gene != ''
        GROUP BY v.gene ORDER BY cnt DESC
    """)
    return jsonify([r[0] for r in rows])

# ── Patients ──────────────────────────────────────────────────────────────────

@app.route("/patients")
def patients():
    _, diag_rows = query("""
        SELECT DISTINCT diagnosis FROM patient_with_age
        WHERE diagnosis IS NOT NULL AND diagnosis != ''
        ORDER BY diagnosis
    """)
    diagnoses = [r[0] for r in diag_rows]

    _, inh_rows = query("""
        SELECT DISTINCT inheritancepattern FROM patient_with_age
        WHERE inheritancepattern IS NOT NULL AND inheritancepattern != ''
        ORDER BY inheritancepattern
    """)
    inheritances = [r[0] for r in inh_rows]

    f_diag = request.args.get("diagnosis", "")
    f_sex  = request.args.get("sex", "")
    f_inh  = request.args.get("inheritance", "")
    f_gene = request.args.get("gene", "").strip()

    sql = """
        SELECT p.sample_id, p.sex, p.currentage,
               p.diagnosis, p.inheritancepattern,
               v1.gene AS gene1, p.variant1_zygosity,
               v2.gene AS gene2, p.variant2_zygosity
        FROM patient_with_age p
        LEFT JOIN variant_table v1 ON p.variant1_id = v1.variant_id
        LEFT JOIN variant_table v2 ON p.variant2_id = v2.variant_id
        WHERE 1=1
    """
    params = []
    if f_diag:
        sql += " AND p.diagnosis = ?"
        params.append(f_diag)
    if f_sex:
        sql += " AND p.sex = ?"
        params.append(f_sex)
    if f_inh:
        sql += " AND p.inheritancepattern = ?"
        params.append(f_inh)
    if f_gene:
        sql += " AND (v1.gene LIKE ? OR v2.gene LIKE ?)"
        params.extend([f"%{f_gene}%", f"%{f_gene}%"])
    sql += " ORDER BY p.sample_id"

    cols, rows = query(sql, params)

    return render_template("patients.html",
                           rows=rows,
                           diagnoses=diagnoses,
                           inheritances=inheritances,
                           f_diag=f_diag, f_sex=f_sex,
                           f_inh=f_inh, f_gene=f_gene,
                           total=len(rows))

@app.route("/patients/export")
def patients_export():
    f_diag = request.args.get("diagnosis", "")
    f_sex  = request.args.get("sex", "")
    f_inh  = request.args.get("inheritance", "")
    f_gene = request.args.get("gene", "").strip()

    sql = """
        SELECT p.sample_id, p.sex, p.currentage,
               p.diagnosis, p.inheritancepattern,
               v1.gene AS gene1, p.variant1_zygosity,
               v2.gene AS gene2, p.variant2_zygosity
        FROM patient_with_age p
        LEFT JOIN variant_table v1 ON p.variant1_id = v1.variant_id
        LEFT JOIN variant_table v2 ON p.variant2_id = v2.variant_id
        WHERE 1=1
    """
    params = []
    if f_diag:
        sql += " AND p.diagnosis = ?"
        params.append(f_diag)
    if f_sex:
        sql += " AND p.sex = ?"
        params.append(f_sex)
    if f_inh:
        sql += " AND p.inheritancepattern = ?"
        params.append(f_inh)
    if f_gene:
        sql += " AND (v1.gene LIKE ? OR v2.gene LIKE ?)"
        params.extend([f"%{f_gene}%", f"%{f_gene}%"])
    sql += " ORDER BY p.sample_id"

    cols, rows = query(sql, params)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Sample ID", "Sex", "Current Age", "Diagnosis",
                "Inheritance", "Gene 1", "Zygosity 1", "Gene 2", "Zygosity 2"])
    w.writerows(rows)

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=patients.csv"}
    )

# ── Variants ──────────────────────────────────────────────────────────────────

@app.route("/variants")
def variants():
    _, path_rows = query("""
        SELECT DISTINCT
            CASE
                WHEN algorithm_prediction LIKE 'Pathogenic%' AND algorithm_prediction NOT LIKE 'Likely%' THEN 'Pathogenic'
                WHEN algorithm_prediction LIKE 'Likely Pathogenic%' THEN 'Likely Pathogenic'
                WHEN algorithm_prediction LIKE 'Uncertain%' THEN 'Uncertain Significance'
                ELSE algorithm_prediction
            END AS cls
        FROM variant_table WHERE algorithm_prediction IS NOT NULL
        ORDER BY cls
    """)
    pathogenicities = [r[0] for r in path_rows if r[0]]

    _, vtype_rows = query("""
        SELECT DISTINCT variant_change_type FROM variant_table
        WHERE variant_change_type IS NOT NULL AND variant_change_type != ''
        ORDER BY variant_change_type
    """)
    vtypes = [r[0] for r in vtype_rows]

    f_gene  = request.args.get("gene", "").strip()
    f_path  = request.args.get("pathogenicity", "")
    f_vtype = request.args.get("vtype", "")

    sql = """
        SELECT variant_id, gene, transcript, hgvsc, protein_change,
               algorithm_prediction, variant_change_type,
               taiwan_biobank_af, gnomad_exome_east_af
        FROM variant_table
        WHERE 1=1
    """
    params = []
    if f_gene:
        sql += " AND gene LIKE ?"
        params.append(f"%{f_gene}%")
    if f_path:
        sql += " AND algorithm_prediction LIKE ?"
        params.append(f"{f_path}%")
    if f_vtype:
        sql += " AND variant_change_type = ?"
        params.append(f_vtype)
    sql += " ORDER BY gene, variant_id"

    cols, rows = query(sql, params)

    return render_template("variants.html",
                           rows=rows,
                           pathogenicities=pathogenicities,
                           vtypes=vtypes,
                           f_gene=f_gene, f_path=f_path,
                           f_vtype=f_vtype, total=len(rows),
                           clinvar=CLINVAR)

@app.route("/variants/export")
def variants_export():
    f_gene  = request.args.get("gene", "").strip()
    f_path  = request.args.get("pathogenicity", "")
    f_vtype = request.args.get("vtype", "")

    sql = """
        SELECT variant_id, gene, transcript, hgvsc, protein_change,
               algorithm_prediction, variant_change_type,
               taiwan_biobank_af, gnomad_exome_east_af
        FROM variant_table WHERE 1=1
    """
    params = []
    if f_gene:
        sql += " AND gene LIKE ?"
        params.append(f"%{f_gene}%")
    if f_path:
        sql += " AND algorithm_prediction LIKE ?"
        params.append(f"{f_path}%")
    if f_vtype:
        sql += " AND variant_change_type = ?"
        params.append(f_vtype)
    sql += " ORDER BY gene, variant_id"

    cols, rows = query(sql, params)

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Variant ID", "Gene", "Transcript", "c", "Protein Change",
                "Pathogenicity", "Variant Type", "Taiwan Biobank AF", "gnomAD East AF"])
    w.writerows(rows)

    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=variants.csv"}
    )
@app.route("/api/filter-options")
def filter_options():
    f_diag = request.args.get("diagnosis", "")
    f_sex  = request.args.get("sex", "")
    f_inh  = request.args.get("inheritance", "")
    f_gene = request.args.get("gene", "").strip()

    def base(exclude=None):
        sql = """
            FROM patient_table p
            LEFT JOIN variant_table v1 ON p.variant1_id = v1.variant_id
            LEFT JOIN variant_table v2 ON p.variant2_id = v2.variant_id
            WHERE 1=1
        """
        params = []
        if exclude != 'diagnosis' and f_diag:
            sql += " AND p.diagnosis = ?"
            params.append(f_diag)
        if exclude != 'sex' and f_sex:
            sql += " AND p.sex = ?"
            params.append(f_sex)
        if exclude != 'inheritance' and f_inh:
            sql += " AND p.inheritancepattern = ?"
            params.append(f_inh)
        if exclude != 'gene' and f_gene:
            sql += " AND (v1.gene LIKE ? OR v2.gene LIKE ?)"
            params.extend([f"%{f_gene}%", f"%{f_gene}%"])
        return sql, params

    s, p = base(exclude='diagnosis')
    _, rows = query(f"SELECT DISTINCT p.diagnosis {s} AND p.diagnosis IS NOT NULL AND p.diagnosis != '' ORDER BY p.diagnosis", p)
    diagnoses = [r[0] for r in rows]

    s, p = base(exclude='sex')
    _, rows = query(f"SELECT DISTINCT p.sex {s} AND p.sex IS NOT NULL AND p.sex != '' ORDER BY p.sex", p)
    sexes = [r[0] for r in rows]

    s, p = base(exclude='inheritance')
    _, rows = query(f"SELECT DISTINCT p.inheritancepattern {s} AND p.inheritancepattern IS NOT NULL AND p.inheritancepattern != '' ORDER BY p.inheritancepattern", p)
    inheritances = [r[0] for r in rows]

    return jsonify(diagnoses=diagnoses, sexes=sexes, inheritances=inheritances)
# ── Search ────────────────────────────────────────────────────────────────────

@app.route("/search")
def search():
    mode = request.args.get("mode", "gene")
    q    = request.args.get("q", "").strip()
    rows = []
    searched = False

    if q:
        searched = True
        if mode == "gene":
            _, rows = query("""
                SELECT p.sample_id, p.diagnosis, p.inheritancepattern,
                       v.gene, v.hgvsc, v.protein_change,
                       v.algorithm_prediction, v.variant_change_type,
                       CASE WHEN p.variant1_id = v.variant_id THEN p.variant1_zygosity
                            ELSE p.variant2_zygosity END AS zygosity
                FROM patient_table p
                JOIN variant_table v ON p.variant1_id = v.variant_id
                                    OR p.variant2_id = v.variant_id
                WHERE v.gene LIKE ?
                ORDER BY p.sample_id
            """, (f"%{q}%",))
        else:
            _, rows = query("""
                SELECT p.sample_id, p.sex, p.birthday,
                    p.diagnosis, p.inheritancepattern,
                    v1.gene, v1.hgvsc, v1.protein_change,
                    v1.algorithm_prediction, p.variant1_zygosity,
                    v2.gene, v2.hgvsc, v2.protein_change,
                    v2.algorithm_prediction, p.variant2_zygosity
                FROM patient_table p
                LEFT JOIN variant_table v1 ON p.variant1_id = v1.variant_id
                LEFT JOIN variant_table v2 ON p.variant2_id = v2.variant_id
                WHERE p.diagnosis = ?
                ORDER BY p.sample_id
            """, (q,))

    return render_template("search.html",
                           mode=mode, q=q,
                           rows=rows, searched=searched)

@app.route("/api/variant-filter-options")
def variant_filter_options():
    f_gene  = request.args.get("gene", "").strip()
    f_path  = request.args.get("pathogenicity", "")
    f_vtype = request.args.get("vtype", "")

    def base(exclude=None):
        sql = " FROM variant_table WHERE 1=1"
        params = []
        if exclude != 'gene' and f_gene:
            sql += " AND gene LIKE ?"
            params.append(f"%{f_gene}%")
        if exclude != 'pathogenicity' and f_path:
            sql += " AND algorithm_prediction LIKE ?"
            params.append(f"{f_path}%")
        if exclude != 'vtype' and f_vtype:
            sql += " AND variant_change_type = ?"
            params.append(f_vtype)
        return sql, params

    s, p = base(exclude='pathogenicity')
    _, rows = query(f"""
        SELECT DISTINCT
            CASE
                WHEN algorithm_prediction LIKE 'Pathogenic%' AND algorithm_prediction NOT LIKE 'Likely%' THEN 'Pathogenic'
                WHEN algorithm_prediction LIKE 'Likely Pathogenic%' THEN 'Likely Pathogenic'
                WHEN algorithm_prediction LIKE 'Uncertain%' THEN 'Uncertain Significance'
                ELSE algorithm_prediction
            END AS cls
        {s} AND algorithm_prediction IS NOT NULL ORDER BY cls
    """, p)
    pathogenicities = [r[0] for r in rows if r[0]]

    s, p = base(exclude='vtype')
    _, rows = query(f"""
        SELECT DISTINCT variant_change_type {s}
        AND variant_change_type IS NOT NULL AND variant_change_type != ''
        ORDER BY variant_change_type
    """, p)
    vtypes = [r[0] for r in rows]

    return jsonify(pathogenicities=pathogenicities, vtypes=vtypes)

@app.route("/autocomplete")
def autocomplete():
    mode = request.args.get("mode", "gene")
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])

    if mode == "gene":
        _, rows = query("""
            SELECT DISTINCT gene
            FROM variant_table
            WHERE gene IS NOT NULL
              AND gene != ''
              AND gene LIKE ?
            ORDER BY gene
            LIMIT 10
        """, (f"%{q}%",))
    else:
        _, rows = query("""
            SELECT DISTINCT diagnosis
            FROM patient_table
            WHERE diagnosis IS NOT NULL
              AND diagnosis != ''
              AND diagnosis LIKE ?
            ORDER BY diagnosis
            LIMIT 10
        """, (f"%{q}%",))

    return jsonify([r[0] for r in rows if r and r[0]])
                           

# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True)
