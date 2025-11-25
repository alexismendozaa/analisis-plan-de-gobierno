import os
import re
import requests
import unicodedata
import tempfile
import pdfplumber
from bs4 import BeautifulSoup
from datetime import datetime
import time
import ollama
import json

class DataScraper:
    def __init__(self, headers=None, rate_limit_seconds=2):
        self.headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.rate_limit_seconds = rate_limit_seconds
        self.año_actual = 2025
        self.model = "llama3.1:8b"

    def quitar_tildes(self, texto):
        if not isinstance(texto, str): return texto
        return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

    def es_pdf_por_url(self, url):
        return url.lower().endswith('.pdf') if url else False

    def identificar_fuentes(self, indicador):
        """Identifica fuentes oficiales según el indicador"""
        indicador_norm = self.quitar_tildes((indicador or '').lower())
        
        fuentes = {
            'pobreza multidimensional': [
                'https://www.ecuadorencifras.gob.ec/documentos/web-inec/POBREZA/2024/Diciembre/202412_PobrezayDesigualdad.pdf'
            ],
            'pobreza extrema por ingresos': [
                'https://www.ecuadorencifras.gob.ec/documentos/web-inec/POBREZA/2025/Junio/202506_Boletin_pobreza_ENEMDU.pdf',
                'https://www.ecuadorencifras.gob.ec/documentos/web-inec/POBREZA/2024/Diciembre/202412_PobrezayDesigualdad.pdf'
            ],
            'pobreza extrema': [
                'https://www.ecuadorencifras.gob.ec/documentos/web-inec/POBREZA/2025/Junio/202506_Boletin_pobreza_ENEMDU.pdf',
                'https://www.ecuadorencifras.gob.ec/documentos/web-inec/POBREZA/2024/Diciembre/202412_PobrezayDesigualdad.pdf'
            ],
            'empleo adecuado': [
                'https://www.ecuadorencifras.gob.ec/empleo-septiembre-2025/'
            ],
            'desempleo': [
                'https://www.ecuadorencifras.gob.ec/documentos/web-inec/EMPLEO/2025/Septiembre/Trimestre_julio-septiembre_2025_Mercado_Laboral.pdf'
            ],
            'inversion extranjera directa': [
                'https://www.produccion.gob.ec/wp-content/uploads/2025/08/BOLETIN-DE-CIFRAS-DE-INVERSIONES-I-TRIMESTRE-2025.pdf'
            ],
            'inversion extranjera': [
                'https://www.produccion.gob.ec/wp-content/uploads/2025/08/BOLETIN-DE-CIFRAS-DE-INVERSIONES-I-TRIMESTRE-2025.pdf'
            ],
            'mortalidad por suicidio': [
                'https://www.ecuadorencifras.gob.ec/documentos/web-inec/Poblacion_y_Demografia/Defunciones_Generales_2023/Boletin_tecnico_EDG_2023.pdf'
            ],
            'siniestros de transito': [
                'https://confirmado.net/tema-accidentes-viales-en-ecuador-dejan-4-000-muertes-al-ano-y-sin-freno-a-la-vista/'
            ],
            'mortalidad': [
                'https://www.ecuadorencifras.gob.ec/defunciones-generales/',
                'https://www.ant.gob.ec/'
            ],
            'internet': [
                'https://www.ecuadorencifras.gob.ec/documentos/web-inec/Estadisticas_Sociales/TIC/2023/230913_Boletin_Tecnico_Multiprop_TIC_2023_VF.pdf',
                'https://www.ecuadorencifras.gob.ec/tecnologias-de-la-informacion-y-comunicacion-tic/'
            ],
            'fibra optica': [
                'https://www.arcotel.gob.ec/estadisticas/',
                'https://www.ecuadorencifras.gob.ec/tecnologias-de-la-informacion-y-comunicacion-tic/'
            ],
            'desnutricion': ['https://www.ecuadorencifras.gob.ec/encuesta-nacional-de-desnutricion-infantil-endi/'],
            'homicidios': ['https://www.ministeriodelinterior.gob.ec/cifras-de-seguridad/'],
            'seguridad': ['https://www.ministeriodelinterior.gob.ec/'],
            'educacion': ['https://www.ecuadorencifras.gob.ec/estadisticas-educativas/'],
            'salud': ['https://www.salud.gob.ec/estadisticas-de-salud-2/'],
            'pib': ['https://www.bce.fin.ec/index.php/boletines-de-prensa-archivo/item/1421-la-economia-ecuatoriana-crecio']
        }
        
        for clave, urls in sorted(fuentes.items(), key=lambda x: len(x[0]), reverse=True):
            if clave in indicador_norm:
                print(f"   ✓ Fuentes identificadas para: '{clave}'")
                return urls
        
        return ['https://www.ecuadorencifras.gob.ec']

    def determinar_rango_esperado(self, indicador, meta):
        """
        Determina el rango razonable de valores según el indicador
        CRÍTICO: Evita confundir unidades (100,000 habitantes) con valores reales
        """
        indicador_lower = indicador.lower()
        meta_lower = str(meta).lower()
        
        # TASAS DE MORTALIDAD/SINIESTROS (por 100k habitantes)
        if any(x in indicador_lower for x in ['mortalidad', 'siniestros', 'accidentes', 'muertes']) and \
           'por cada 100' in meta_lower:
            return {
                'min': 0.1,
                'max': 100.0,  # Tasas usualmente entre 0.1 y 100
                'tipo': 'tasa_por_100k',
                'unidad': 'por cada 100,000 hab',
                'excluir': [100, 1000, 10000, 100000]  # Números que son unidades, no datos
            }
        
        # PORCENTAJES GENERALES
        elif '%' in meta_lower or any(x in indicador_lower for x in ['tasa', 'porcentaje', 'proporción']):
            return {
                'min': 0.01,
                'max': 100.0,
                'tipo': 'porcentaje',
                'unidad': '%',
                'excluir': [100, 1000]  # 100 puede ser "por 100 habitantes"
            }
        
        # VALORES MONETARIOS (millones USD)
        elif 'millones' in meta_lower or 'usd' in meta_lower or 'inversion' in indicador_lower:
            return {
                'min': 1.0,
                'max': 10000.0,
                'tipo': 'monetario',
                'unidad': 'millones USD',
                'excluir': []
            }
        
        # VALORES ABSOLUTOS (casos, personas, etc)
        else:
            return {
                'min': 1,
                'max': 1000000,
                'tipo': 'absoluto',
                'unidad': 'casos/personas',
                'excluir': [100, 1000, 10000, 100000]  # Números redondos probablemente son unidades
            }

    def extraer_texto_completo_pdf(self, url, timeout=60):
        """Extrae TODO el texto del PDF sin límites"""
        try:
            print(f"      📥 Descargando PDF completo...")
            resp = requests.get(url, headers=self.headers, timeout=timeout, stream=True)
            
            if resp.status_code != 200:
                print(f"      ⚠️ HTTP {resp.status_code}")
                return None
            
            with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp:
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                tmp_path = tmp.name
            
            print(f"      📖 Leyendo PDF completo...")
            texto_completo = []
            
            with pdfplumber.open(tmp_path) as pdf:
                total_pages = len(pdf.pages)
                print(f"      📄 Total de páginas: {total_pages}")
                
                for i, page in enumerate(pdf.pages, 1):
                    page_text = page.extract_text(layout=True) or ""
                    texto_completo.append(page_text)
                    if i % 5 == 0:
                        print(f"         Procesadas {i}/{total_pages} páginas...")
            
            os.remove(tmp_path)
            full_text = "\n".join(texto_completo)
            print(f"      ✅ Extraído: {len(full_text):,} caracteres de {total_pages} páginas")
            
            return full_text
            
        except Exception as e:
            print(f"      ❌ Error en PDF: {e}")
            return None

    def extraer_con_ollama_inteligente(self, texto_completo, indicador, meta):
        """
        USA OLLAMA PARA LEER Y ENTENDER EL DOCUMENTO COMPLETO
        MEJORADO: Con validación de rangos y exclusión de valores de unidades
        """
        if not texto_completo or len(texto_completo) < 100:
            print("      ⚠️ Texto insuficiente para análisis con IA")
            return []
        
        # Determinar rango esperado ANTES de extraer
        rango = self.determinar_rango_esperado(indicador, meta)
        print(f"      🎯 Rango esperado: {rango['min']}-{rango['max']} {rango['unidad']}")
        if rango['excluir']:
            print(f"      🚫 Valores a IGNORAR (son unidades, no datos): {rango['excluir']}")
        
        # Limitar texto si es muy largo
        max_chars = 15000
        if len(texto_completo) > max_chars:
            texto_analisis = texto_completo[:max_chars//2] + "\n...\n" + texto_completo[-max_chars//2:]
            print(f"      📝 Texto reducido a {len(texto_analisis):,} caracteres")
        else:
            texto_analisis = texto_completo
        
        # Construir prompt MEJORADO con instrucciones de exclusión
        prompt = f"""Eres un experto analista de datos estadísticos oficiales de Ecuador.

TAREA: Extraer el valor MÁS RECIENTE del siguiente indicador.

INDICADOR BUSCADO: {indicador}
META DEL GOBIERNO: {meta}
TIPO DE DATO: {rango['tipo']}
UNIDAD: {rango['unidad']}
RANGO VÁLIDO: {rango['min']} - {rango['max']}

⚠️ VALORES QUE DEBES IGNORAR (son parte de las UNIDADES, NO son datos):
{rango['excluir']}

Ejemplo: Si el texto dice "12.81 por cada 100,000 habitantes", el dato es 12.81, NO 100 ni 100000.

DOCUMENTO:
{texto_analisis}

INSTRUCCIONES CRÍTICAS:
1. Lee TODO el documento buscando el indicador específico: "{indicador}"
2. IGNORA números que sean parte de unidades (como 100, 1000, 100000)
3. El valor debe estar entre {rango['min']} y {rango['max']}
4. Prioriza datos de 2025, luego 2024
5. VERIFICA que el número sea del indicador correcto

RESPONDE EN JSON:
{{
    "valor_encontrado": <número sin símbolos, solo el DATO real>,
    "año": <año del dato>,
    "mes": "<mes si está disponible>",
    "contexto": "<frase del documento (máximo 200 caracteres)>",
    "confianza": <1-10>,
    "tipo_dato": "{rango['tipo']}",
    "unidad": "{rango['unidad']}"
}}

Si NO encuentras un valor válido:
{{"valor_encontrado": null, "razon": "explicación"}}
"""
        
        try:
            print(f"\n      🤖 OLLAMA analizando con filtros anti-confusión...")
            
            respuesta = ollama.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": "Eres un analista experto. Respondes SOLO en JSON válido. NO confundes unidades con datos."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            respuesta_text = respuesta.get("message", {}).get("content", "").strip()
            
            # Limpiar respuesta
            respuesta_text = re.sub(r'^```json\s*', '', respuesta_text)
            respuesta_text = re.sub(r'\s*```$', '', respuesta_text)
            
            resultado = json.loads(respuesta_text)
            
            if resultado.get("valor_encontrado") is not None:
                valor = float(resultado["valor_encontrado"])
                
                # VALIDACIÓN CRÍTICA: Rechazar valores en lista de exclusión
                if valor in rango['excluir']:
                    print(f"      🚫 VALOR RECHAZADO: {valor} está en lista de exclusión (es una unidad)")
                    return []
                
                # VALIDACIÓN: Verificar que esté en rango
                if not (rango['min'] <= valor <= rango['max']):
                    print(f"      🚫 VALOR RECHAZADO: {valor} fuera de rango {rango['min']}-{rango['max']}")
                    return []
                
                año = resultado.get("año")
                confianza = resultado.get("confianza", 5)
                
                print(f"      ✅ IA EXTRAJO VÁLIDO: {valor} {resultado.get('unidad', '')} (Año: {año}, Confianza: {confianza}/10)")
                print(f"         Contexto: {resultado.get('contexto', 'N/A')[:150]}...")
                
                relevancia = confianza * 10
                if año == 2025:
                    relevancia += 30
                elif año == 2024:
                    relevancia += 15
                
                return [{
                    'valor': valor,
                    'texto_raw': resultado.get('contexto', '')[:100],
                    'contexto': resultado.get('contexto', ''),
                    'tipo': resultado.get('tipo_dato', 'generico'),
                    'año': año,
                    'mes': resultado.get('mes'),
                    'relevancia': relevancia,
                    'confianza_ia': confianza,
                    'unidad': resultado.get('unidad', ''),
                    'metodo': 'ollama_inteligente'
                }]
            else:
                print(f"      ⚠️ IA no encontró valor: {resultado.get('razon', 'Sin razón')}")
                return []
                
        except json.JSONDecodeError as e:
            print(f"      ❌ Error JSON: {e}")
            print(f"         Respuesta: {respuesta_text[:300]}")
            return []
        except Exception as e:
            print(f"      ❌ Error en IA: {e}")
            import traceback
            traceback.print_exc()
            return []

    def extraer_valores_fallback_regex(self, texto, indicador, meta):
        """
        Sistema de respaldo con regex MEJORADO
        Incluye validación de rangos
        """
        print(f"      ⚙️ Usando extracción regex de respaldo...")
        
        rango = self.determinar_rango_esperado(indicador, meta)
        resultados = []
        indicador_lower = indicador.lower()
        
        # Patrones según tipo de indicador
        if rango['tipo'] == 'tasa_por_100k':
            # CRÍTICO: Capturar SOLO la tasa, NO los 100,000
            patrones = [
                (r'(?:fue|alcanzó|registró|ubicó)\s+(?:de|en)?\s*(\d+[.,]\d+)\s*(?:por\s+cada|cada)\s*100', 'tasa_por_100k'),
                (r'(\d+[.,]\d+)\s*(?:por\s+cada|cada)\s*100[.,]?000', 'tasa_por_100k'),
                (r'tasa\s+(?:de\s+)?(?:mortalidad|siniestros|accidentes)\s+(?:fue|es)?\s*(\d+[.,]\d+)', 'tasa_por_100k')
            ]
        elif rango['tipo'] == 'porcentaje':
            patrones = [
                (r'(?:fue|alcanzó|llegó|registró|ubicó)\s+(?:de|en)?\s*(\d+[.,]\d+)\s*%', 'porcentaje'),
                (r'(\d+[.,]\d+)\s*%', 'porcentaje')
            ]
        elif rango['tipo'] == 'monetario':
            patrones = [
                (r'USD\s*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?)\s*millones?', 'monetario'),
                (r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?)\s*millones?\s*(?:de\s+)?USD', 'monetario')
            ]
        else:
            patrones = [
                (r'(\d+[.,]\d+)\s*(?:casos|personas|hogares)', 'absoluto')
            ]
        
        for patron, tipo in patrones:
            for match in re.finditer(patron, texto, re.IGNORECASE):
                try:
                    val_str = match.group(1).replace('.', '').replace(',', '.')
                    val = float(val_str)
                    
                    # VALIDACIÓN: Rechazar valores excluidos
                    if val in rango['excluir']:
                        continue
                    
                    # VALIDACIÓN: Verificar rango
                    if not (rango['min'] <= val <= rango['max']):
                        continue
                    
                    start = max(0, match.start() - 300)
                    end = min(len(texto), match.end() + 300)
                    contexto = texto[start:end]
                    
                    año_match = re.search(r'(202[0-5])', contexto)
                    año = int(año_match.group(1)) if año_match else None
                    
                    relevancia = 5
                    if año == 2025:
                        relevancia += 20
                    elif año == 2024:
                        relevancia += 10
                    
                    palabras_indicador = [p for p in indicador_lower.split() if len(p) > 3]
                    for palabra in palabras_indicador:
                        if palabra in contexto.lower():
                            relevancia += 5
                    
                    resultados.append({
                        'valor': val,
                        'texto_raw': match.group(0),
                        'contexto': contexto,
                        'tipo': tipo,
                        'año': año,
                        'relevancia': relevancia,
                        'metodo': 'regex_fallback'
                    })
                except:
                    continue
        
        resultados.sort(key=lambda x: (x.get('año', 0) == 2025, x.get('relevancia', 0)), reverse=True)
        
        if resultados:
            print(f"      ✅ Regex encontró {len(resultados)} candidatos válidos")
        
        return resultados[:5]

    def buscar_datos(self, indicador, meta=""):
        """
        Búsqueda inteligente con validación de rangos
        """
        fuentes = self.identificar_fuentes(indicador)
        
        print(f"\n{'='*70}")
        print(f"🔍 BÚSQUEDA: {indicador}")
        print(f"🎯 Meta: {meta}")
        print(f"{'='*70}")
        
        resultados_finales = []
        
        for idx, url in enumerate(fuentes, 1):
            print(f"\n[{idx}/{len(fuentes)}] 🌐 {url}")
            
            try:
                if self.es_pdf_por_url(url):
                    texto_completo = self.extraer_texto_completo_pdf(url)
                    
                    if texto_completo:
                        # Método 1: IA con validación
                        valores_ia = self.extraer_con_ollama_inteligente(texto_completo, indicador, meta)
                        
                        if valores_ia:
                            resultados_finales.append({
                                'fuente': url,
                                'numeros_contexto': valores_ia,
                                'fechas_encontradas': [f"{v.get('mes', 'Año')} {v.get('año', '?')}" for v in valores_ia],
                                'tiene_datos_2025': any(v.get('año') == 2025 for v in valores_ia),
                                'metodo_principal': 'ollama'
                            })
                        else:
                            # Método 2: Regex con validación
                            valores_regex = self.extraer_valores_fallback_regex(texto_completo, indicador, meta)
                            
                            if valores_regex:
                                resultados_finales.append({
                                    'fuente': url,
                                    'numeros_contexto': valores_regex,
                                    'fechas_encontradas': [f"Año {v.get('año', '?')}" for v in valores_regex],
                                    'tiene_datos_2025': any(v.get('año') == 2025 for v in valores_regex),
                                    'metodo_principal': 'regex_fallback'
                                })
                else:
                    # Páginas web
                    resp = requests.get(url, headers=self.headers, timeout=20)
                    soup = BeautifulSoup(resp.content, 'html.parser')
                    
                    for script in soup(["script", "style", "nav", "footer"]):
                        script.decompose()
                    
                    texto = soup.get_text(separator=' ', strip=True)
                    
                    if texto and len(texto) > 200:
                        valores_ia = self.extraer_con_ollama_inteligente(texto, indicador, meta)
                        
                        if valores_ia:
                            resultados_finales.append({
                                'fuente': url,
                                'numeros_contexto': valores_ia,
                                'fechas_encontradas': [f"{v.get('mes', 'Año')} {v.get('año', '?')}" for v in valores_ia],
                                'tiene_datos_2025': any(v.get('año') == 2025 for v in valores_ia),
                                'metodo_principal': 'ollama'
                            })
                        else:
                            valores_regex = self.extraer_valores_fallback_regex(texto, indicador, meta)
                            if valores_regex:
                                resultados_finales.append({
                                    'fuente': url,
                                    'numeros_contexto': valores_regex,
                                    'fechas_encontradas': [f"Año {v.get('año', '?')}" for v in valores_regex],
                                    'tiene_datos_2025': any(v.get('año') == 2025 for v in valores_regex),
                                    'metodo_principal': 'regex_fallback'
                                })
                
                time.sleep(self.rate_limit_seconds)
                
            except Exception as e:
                print(f"      ❌ Error: {e}")
                continue
        
        print(f"\n{'='*70}")
        print(f"✅ BÚSQUEDA COMPLETADA: {len(resultados_finales)} fuentes con datos válidos")
        print(f"{'='*70}\n")
        
        return resultados_finales