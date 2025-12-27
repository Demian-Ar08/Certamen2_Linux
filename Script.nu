# =========================================================
# script.nu
# Procesamiento de carreras de Estadística y Ciencia de Datos
# =========================================================

# ---------------------------------------------------------
# 1. Carga, limpieza inicial y normalización de columnas
# ---------------------------------------------------------

let base = (
    open --raw estudio_mercado_ordenado.csv
    | from csv --separator ';'
    | compact --empty
    | rename "AÑO" anio
             "NOMBRE INSTITUCION" institucion
             "NOMBRE SEDE" sede
             "NOMBRE CARRERA" carrera
             "TOTAL MATRICULADOS" matricula_total
             "TOTAL MATRICULADOS PRIMER AÑO" matricula_primer_ano
    | update anio { $in | into string | str replace "MAT_" "" | into int }
    | where {|r| $r.anio >= 2020 and $r.anio <= 2024 }
)

# Guardar base intermedia 2020–2024
$base | save ies_base_2020_2024.csv


# ---------------------------------------------------------
# 2. Filtrado de carreras de Estadística y Ciencia de Datos
# ---------------------------------------------------------

let filtradas = (
    $base
    | where {|r|
        (
            ($r.carrera | into string | str downcase | str contains "estad")
            or
            ($r.carrera | into string | str downcase | str contains "dato")
        )
    }
    | where {|r|
        not (
            ($r.carrera | into string | str downcase | str contains "magister")
            or ($r.carrera | into string | str downcase | str contains "magíster")
            or ($r.carrera | into string | str downcase | str contains "doctor")
            or ($r.carrera | into string | str downcase | str contains "post")
            or ($r.carrera | into string | str downcase | str contains "diplom")
            or ($r.carrera | into string | str downcase | str contains "licenc")
            or ($r.carrera | into string | str downcase | str contains "regulariz")
            or ($r.carrera | into string | str downcase | str contains "pedagog")
        )
    }
)

# Guardar dataset filtrado
$filtradas | save ies_final_estadistica_datos_2020_2024.csv


# ---------------------------------------------------------
# 3. Reestructuración (pivoteo manual por año)
# ---------------------------------------------------------

$filtradas
| group-by institucion sede carrera
| items { |key, reg|
    {
        "NOMBRE INSTITUCIÓN": ($key | get 0),
        "NOMBRE SEDE": ($key | get 1),
        "NOMBRE CARRERA": ($key | get 2),

        "TOTAL_2020": ($reg | where anio == 2020 | get 0?.matricula_total | default 0),
        "TOTAL_2021": ($reg | where anio == 2021 | get 0?.matricula_total | default 0),
        "TOTAL_2022": ($reg | where anio == 2022 | get 0?.matricula_total | default 0),
        "TOTAL_2023": ($reg | where anio == 2023 | get 0?.matricula_total | default 0),
        "TOTAL_2024": ($reg | where anio == 2024 | get 0?.matricula_total | default 0),

        "PRIMER_2020": ($reg | where anio == 2020 | get 0?.matricula_primer_ano | default 0),
        "PRIMER_2021": ($reg | where anio == 2021 | get 0?.matricula_primer_ano | default 0),
        "PRIMER_2022": ($reg | where anio == 2022 | get 0?.matricula_primer_ano | default 0),
        "PRIMER_2023": ($reg | where anio == 2023 | get 0?.matricula_primer_ano | default 0),
        "PRIMER_2024": ($reg | where anio == 2024 | get 0?.matricula_primer_ano | default 0)
    }
}
| save ies_last_5_years_data.csv


