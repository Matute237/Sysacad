# scripts/importar_xml_generico.py
import os
import sys
from xml.etree import ElementTree as ET
from typing import Optional, Dict, Any, Callable

from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.sqltypes import Integer, BigInteger, Float, Boolean, String, Text

# --- Bootstrap path del proyecto ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from app import create_app, db


def get_text(elem):
    """Devuelve texto limpio o None."""
    return elem.text.strip() if (elem is not None and elem.text) else None


def definir_tipo_de_valor(value: Optional[str], col_type) -> Any:
    """Convierte string XML al tipo de la columna SQLAlchemy (int, float, bool, str)."""
    if value is None:
        return None
    if isinstance(col_type, (String, Text)):
        return value
    if isinstance(col_type, (Integer, BigInteger)):
        return int(value)
    if isinstance(col_type, Float):
        return float(value)
    if isinstance(col_type, Boolean):
        v = value.lower()
        return v in ("1", "true", "t", "yes", "y", "si", "sí")
    # fallback: string
    return value


def _model_columns(model) -> Dict[str, Any]:
    """Devuelve {nombre_columna: tipo_sqlalchemy} del modelo."""
    return {col.name: col.type for col in model.__table__.columns}


def importar_datos(
    nombre_archivo: str,
    model,
    item_tag: str = "_exportar",
    field_map: Optional[Dict[str, str]] = None,
    pk_from: Optional[str] = None,
    transforms: Optional[Dict[str, Callable[[Optional[str]], Any]]] = None,
):
    """
    Importa datos desde un XML a la tabla del 'model' (SQLAlchemy).

    Parámetros:
    - nombre_archivo: nombre del XML dentro de 'archivados_xml' (o ruta absoluta)
    - model: clase del modelo SQLAlchemy (p.ej. Localidad)
    - item_tag: nombre del nodo que representa un registro (default: '_exportar')
    - field_map: dict opcional {col_modelo: tag_xml} para tags con nombre distinto
    - pk_from: si la PK del modelo (p. ej. 'id') debe tomarse desde otro tag del XML (p. ej. 'codigo')
    - transforms: dict opcional {col_modelo: fn(str)->valor} para transformar valores antes de guardar
    """
    field_map = field_map or {}
    transforms = transforms or {}

    # Si te interesa forzar el path dentro de archivados_xml:
    XML_RELATIVE_PATH = (
        nombre_archivo if os.path.isabs(nombre_archivo)
        else os.path.join('archivados_xml', nombre_archivo)
    )

    app = create_app()
    with app.app_context():
        db.create_all()

        xml_file_path = os.path.abspath(os.path.join(BASE_DIR, XML_RELATIVE_PATH))
        if not os.path.exists(xml_file_path):
            print(f"ERROR: No se encontró el archivo XML: {xml_file_path}")
            return

        print(f"Importando {model.__name__} desde: {xml_file_path}")

        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
        except ET.ParseError as e:
            print(f"Error al parsear el archivo XML: {e}")
            return

        cols = _model_columns(model)
        pk_name = next((c.name for c in model.__table__.primary_key.columns), "id")

        insertados = 0
        duplicados = 0
        errores = 0

        for item in root.findall(item_tag):
            try:
                data = {}
                for col_name, col_type in cols.items():
                    # tag de origen: field_map > mismo nombre
                    xml_tag = field_map.get(col_name, col_name)

                    # Si la columna es PK y se indicó pk_from, tomar de ese tag:
                    if col_name == pk_name and pk_from:
                        xml_tag = pk_from

                    elem = item.find(xml_tag)
                    raw = get_text(elem)

                    # transformación custom si aplica
                    if col_name in transforms:
                        val = transforms[col_name](raw)
                    else:
                        val = definir_tipo_de_valor(raw, col_type)

                    data[col_name] = val

                # Validar PK
                pk_val = data.get(pk_name)
                if pk_val is None:
                    raise ValueError(f"El registro no tiene PK '{pk_name}' definida (model={model.__name__}).")

                # Evitar duplicados
                if db.session.get(model, pk_val):
                    print(f"Duplicado ID {pk_val}")
                    duplicados += 1
                    continue

                obj = model(**data)
                db.session.add(obj)
                db.session.commit()
                insertados += 1

            except ValueError as ve:
                db.session.rollback()
                print(f"Error de valor: {ve}")
                errores += 1
            except IntegrityError:
                db.session.rollback()
                print(f"Error de integridad al insertar ID {data.get(pk_name)}")
                errores += 1
            except Exception as e:
                db.session.rollback()
                print(f"Error procesando item: {e}")
                errores += 1

        print(f"""
Importación finalizada ({model.__name__}):
- Registros insertados: {insertados}
- Registros duplicados: {duplicados}
- Registros con error: {errores}
""")

