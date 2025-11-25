from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import os
from scraper import DataScraper
from analyzer import AIAnalyzer
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

app = Flask(__name__)
CORS(app)

scraper = DataScraper()
analyzer = AIAnalyzer()

try:
    MAX_ANALYSIS_WORKERS = max(1, int(os.getenv('MAX_ANALYSIS_WORKERS', '2')))  # Reducido por análisis IA
except ValueError:
    MAX_ANALYSIS_WORKERS = 2

def _obtener_valor_actual_inteligente(datos_scraping, indicador):
    """
    Selecciona el valor MÁS CONFIABLE extraído por Ollama o regex
    Prioridad: 
    1. Ollama con alta confianza (8-10) y año 2025
    2. Ollama con confianza media (5-7) y año 2025
    3. Ollama año 2024
    4. Regex con alta relevancia
    """
    if not datos_scraping:
        print("      ⚠️ No hay datos de scraping")
        return None
    
    numeros_contexto = []
    for resultado in datos_scraping:
        numeros_contexto.extend(resultado.get('numeros_contexto', []))
    
    if not numeros_contexto:
        print("      ⚠️ No hay números en contexto")
        return None
    
    print(f"\n      🎯 SELECCIÓN INTELIGENTE DE VALOR:")
    print(f"         Candidatos totales: {len(numeros_contexto)}")
    
    # Separar por método de extracción
    valores_ia = [n for n in numeros_contexto if n.get('metodo') == 'ollama_inteligente']
    valores_regex = [n for n in numeros_contexto if n.get('metodo') == 'regex_fallback']
    
    print(f"         - Extraídos por IA (Ollama): {len(valores_ia)}")
    print(f"         - Extraídos por Regex: {len(valores_regex)}")
    
    # PRIORIDAD 1: Valores de IA con alta confianza
    if valores_ia:
        # Ordenar por: año (2025 primero), luego confianza
        valores_ia_ordenados = sorted(
            valores_ia,
            key=lambda x: (
                x.get('año', 0) == 2025,
                x.get('confianza_ia', 0),
                x.get('relevancia', 0)
            ),
            reverse=True
        )
        
        mejor_ia = valores_ia_ordenados[0]
        confianza = mejor_ia.get('confianza_ia', 0)
        
        print(f"\n         🤖 MEJOR VALOR DE IA:")
        print(f"            Valor: {mejor_ia['valor']} {mejor_ia.get('unidad', '')}")
        print(f"            Año: {mejor_ia.get('año', '?')}")
        print(f"            Confianza: {confianza}/10")
        print(f"            Contexto: {mejor_ia.get('contexto', '')[:100]}...")
        
        # Si confianza es alta (≥6), usar ese valor
        if confianza >= 6:
            print(f"         ✅ SELECCIONADO (Alta confianza IA)")
            return mejor_ia['valor']
    
    # PRIORIDAD 2: Si IA tiene baja confianza, verificar regex
    if valores_regex:
        valores_regex_ordenados = sorted(
            valores_regex,
            key=lambda x: (
                x.get('año', 0) == 2025,
                x.get('relevancia', 0)
            ),
            reverse=True
        )
        
        mejor_regex = valores_regex_ordenados[0]
        print(f"\n         ⚙️ MEJOR VALOR DE REGEX:")
        print(f"            Valor: {mejor_regex['valor']} ({mejor_regex.get('tipo', '?')})")
        print(f"            Año: {mejor_regex.get('año', '?')}")
        print(f"            Relevancia: {mejor_regex.get('relevancia', 0)}")
        
        # Si hay IA pero baja confianza, comparar
        if valores_ia:
            mejor_ia = valores_ia_ordenados[0]
            if mejor_ia.get('confianza_ia', 0) < 6 and mejor_regex.get('relevancia', 0) > 15:
                print(f"         ✅ SELECCIONADO (Regex más confiable que IA)")
                return mejor_regex['valor']
            else:
                print(f"         ✅ SELECCIONADO (IA preferida sobre regex)")
                return mejor_ia['valor']
        else:
            print(f"         ✅ SELECCIONADO (Único método: regex)")
            return mejor_regex['valor']
    
    # FALLBACK: Si solo hay IA con baja confianza
    if valores_ia:
        print(f"         ⚠️ SELECCIONADO (IA única opción, baja confianza)")
        return valores_ia_ordenados[0]['valor']
    
    print(f"         ❌ No se pudo seleccionar valor confiable")
    return None


@app.route('/')
def index():
    return "API Plan de Gobierno Monitor - v5.0 OLLAMA INTELIGENTE"

@app.route('/api/health', methods=['GET'])
def health():
    ollama_ok = analyzer.verificar_ollama()
    excel_exists = os.path.exists('../data/plan_gobierno_2025_2029.xlsx') or \
                   os.path.exists('data/plan_gobierno_2025_2029.xlsx')
    
    return jsonify({
        'status': 'ok' if ollama_ok else 'warning',
        'ollama': 'funcionando' if ollama_ok else 'error - Ejecuta: ollama serve',
        'excel': 'encontrado' if excel_exists else 'no encontrado',
        'version': '5.0 - Extracción con IA Contextual (Ollama)',
        'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    })

@app.route('/api/load-excel', methods=['GET'])
def load_excel():
    try:
        excel_path = '../data/plan_gobierno_2025_2029.xlsx'
        if not os.path.exists(excel_path):
            excel_path = 'data/plan_gobierno_2025_2029.xlsx'
        
        if not os.path.exists(excel_path):
            return jsonify({'success': False, 'error': f'Archivo no encontrado: {excel_path}'}), 404
        
        df = pd.read_excel(excel_path)
        data = df.to_dict('records')
        print(f"\n📊 Excel cargado: {len(data)} indicadores")
        
        return jsonify({'success': True, 'data': data, 'total': len(data)})
        
    except Exception as e:
        print(f"❌ Error cargando Excel: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/analyze', methods=['POST'])
def analyze_indicators():
    try:
        data = request.json
        indicators = data.get('indicators', [])
        
        if not indicators:
            return jsonify({'success': False, 'error': 'No se recibieron indicadores'}), 400
        
        print(f"\n{'='*80}")
        print(f"🚀 ANÁLISIS CON OLLAMA - {len(indicators)} INDICADORES")
        print(f"{'='*80}")
        
        total = len(indicators)
        worker_limit = max(1, min(MAX_ANALYSIS_WORKERS, total))
        print(f"⚙️ Procesamiento: {worker_limit} hilos (limitado por análisis IA)")
        print(f"🤖 Método: Ollama lee documentos completos y extrae datos con contexto")
        results = [None] * total

        def _process_indicator(idx, total_indicators, row_data):
            local_scraper = DataScraper()
            local_analyzer = AIAnalyzer()
            
            print(f"\n{'#'*80}")
            print(f"📋 INDICADOR {idx}/{total_indicators}")
            print(f"{'#'*80}")
            
            eje = row_data.get('Eje', 'Sin eje')
            indicador = row_data.get('Indicador', 'Sin indicador')
            meta = row_data.get('Meta', 'Sin meta')
            
            print(f"📌 {indicador}")
            print(f"🎯 {meta}")

            # Extraer valor inicial
            valor_inicial = row_data.get('ValorInicial')
            if valor_inicial is None:
                try:
                    if isinstance(row_data.get('Meta'), (int, float)):
                        valor_inicial = float(row_data.get('Meta'))
                    else:
                        m = re.search(r'(\d+[.,]?\d*)', str(row_data.get('Meta') or ''))
                        if m:
                            valor_inicial = float(m.group(1).replace(',', '.'))
                except:
                    valor_inicial = None
            
            print(f"📊 Valor Inicial (Base): {valor_inicial}")

            # Scraping inteligente CON META (para contexto)
            try:
                print(f"\n🔎 Iniciando búsqueda web inteligente...")
                datos_scraping = local_scraper.buscar_datos(indicador, meta)
                valor_actual = _obtener_valor_actual_inteligente(datos_scraping, indicador)
            except Exception as e:
                print(f"❌ Error en scraping: {e}")
                import traceback
                traceback.print_exc()
                datos_scraping = []
                valor_actual = None
            
            print(f"\n📊 VALOR ACTUAL FINAL: {valor_actual}")

            # Análisis
            try:
                analysis = local_analyzer.analizar_indicador(
                    eje=eje,
                    indicador=indicador,
                    meta=meta,
                    valor_inicial=valor_inicial,
                    valor_actual=valor_actual,
                    datos_scraping=datos_scraping,
                    contexto=indicador
                )
                
                progreso = analysis.get('progreso', 0)
                print(f"\n✅ ANÁLISIS COMPLETADO")
                print(f"   Progreso: {progreso}%")
                print(f"   Estado: {analysis.get('estado', 'N/A')}")
                print(f"{'#'*80}\n")
                
                return idx, {
                    'eje': eje,
                    'indicador': indicador,
                    'meta': meta,
                    'valor_inicial': analysis.get('valor_inicial', 'No disponible'),
                    'valor_actual': analysis.get('valor_actual', 'No disponible'),
                    **analysis
                }
                
            except Exception as e:
                print(f"❌ Error en análisis: {e}")
                import traceback
                traceback.print_exc()
                return idx, {
                    'eje': eje,
                    'indicador': indicador,
                    'meta': meta,
                    'valor_inicial': valor_inicial or 'No disponible',
                    'valor_actual': 'Error',
                    'progreso': 0,
                    'estado': 'error',
                    'eficiencia': 'N/A',
                    'analisis': f"Error: {str(e)}",
                    'fuente': 'Error'
                }

        # Ejecución paralela con timeout extendido (IA es más lenta)
        with ThreadPoolExecutor(max_workers=worker_limit) as executor:
            futures = {
                executor.submit(_process_indicator, idx, total, row): (idx, row)
                for idx, row in enumerate(indicators, start=1)
            }
            
            for future in as_completed(futures):
                idx, row_data = futures[future]
                try:
                    _, result = future.result(timeout=600)  # 10 min por indicador (IA puede tardar)
                    results[idx - 1] = result
                except Exception as exc:
                    print(f"\n❌ ERROR CRÍTICO en indicador {idx}: {exc}")
                    results[idx - 1] = {
                        'eje': row_data.get('Eje', 'Sin eje'),
                        'indicador': row_data.get('Indicador', 'Sin indicador'),
                        'meta': row_data.get('Meta', 'Sin meta'),
                        'valor_inicial': 'Error',
                        'valor_actual': 'Error',
                        'progreso': 0,
                        'estado': 'error',
                        'eficiencia': 'N/A',
                        'analisis': f"Timeout o error: {exc}",
                        'fuente': 'Error'
                    }

        print(f"\n{'='*80}")
        print(f"✅ ANÁLISIS COMPLETADO")
        print(f"   Indicadores procesados: {len(results)}")
        exitosos = sum(1 for r in results if r and r.get('estado') != 'error')
        print(f"   Exitosos: {exitosos}/{len(results)}")
        print(f"{'='*80}\n")
        
        return jsonify({'success': True, 'results': results})
        
    except Exception as e:
        print(f"\n❌ ERROR GENERAL DEL SISTEMA: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    print("\n" + "="*80)
    print("🚀 SERVIDOR DE ANÁLISIS - PLAN DE GOBIERNO ECUADOR")
    print("="*80)
    print("📋 Versión: 5.0 OLLAMA INTELIGENTE")
    print("🎯 Características:")
    print("   ✓ Ollama LEE documentos completos (PDFs y HTML)")
    print("   ✓ Extracción contextual con IA (no solo regex)")
    print("   ✓ Validación semántica automática")
    print("   ✓ Prioriza datos 2025 con confianza IA")
    print("   ✓ Sistema de doble verificación (IA + Regex)")
    print("   ✓ Análisis contextual profundo")
    print("="*80 + "\n")
    
    # Verificar Ollama
    if analyzer.verificar_ollama():
        print("✅ Ollama conectado correctamente\n")
    else:
        print("⚠️ WARNING: Ollama no está corriendo. Ejecuta: ollama serve\n")
    
    app.run(debug=True, port=5050, host='0.0.0.0')