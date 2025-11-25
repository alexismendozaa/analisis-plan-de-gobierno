import ollama
import json
import re
from datetime import datetime

class AIAnalyzer:
    def __init__(self):
        self.model = "llama3.1:8b"

    def verificar_ollama(self):
        try:
            ollama.chat(model=self.model, messages=[{'role': 'user', 'content': 'ping'}])
            return True
        except:
            return False

    def _parse_num(self, s):
        """Parseador robusto de números"""
        if s is None: return None
        if isinstance(s, (int, float)): return float(s)
        try:
            s = str(s)
            cleaned = re.sub(r"[^0-9.,-]", "", s)
            if ',' in cleaned and '.' in cleaned:
                if cleaned.find(',') < cleaned.find('.'):
                    cleaned = cleaned.replace(',', '')
                else:
                    cleaned = cleaned.replace('.', '').replace(',', '.')
            elif ',' in cleaned:
                cleaned = cleaned.replace(',', '.')
            return float(cleaned)
        except:
            return None

    def analizar_tipo_indicador(self, indicador, meta):
        """Analiza el tipo de indicador y dirección esperada"""
        indicador_lower = indicador.lower() if indicador else ""
        meta_lower = str(meta).lower() if meta else ""
        texto_completo = f"{indicador_lower} {meta_lower}"
        
        palabras_reducir = ['reducir', 'disminuir', 'bajar', 'decrementar', 'minimizar']
        palabras_incrementar = ['incrementar', 'aumentar', 'elevar', 'subir', 'maximizar', 'mejorar']
        
        direccion = "incrementar"
        for palabra in palabras_reducir:
            if palabra in texto_completo:
                direccion = "reducir"
                break
        
        for palabra in palabras_incrementar:
            if palabra in texto_completo:
                direccion = "incrementar"
                break
        
        tipo = "numero"
        unidad = ""
        
        # MEJORADO: Detectar "por cada 100,000" como tasa
        if 'por cada 100' in texto_completo or 'cada 100.000' in texto_completo:
            tipo = "tasa"
            unidad = "por 100k hab"
            direccion = "reducir"  # Tasas de mortalidad/siniestros se reducen
        elif any(x in texto_completo for x in ['%', 'porcentaje', 'tasa']):
            tipo = "porcentaje"
            unidad = "%"
        elif any(x in indicador_lower for x in ['pobreza', 'desempleo', 'inflación', 'mortalidad', 
                                                  'accidente', 'delito', 'deserción', 'déficit']):
            direccion = "reducir"
            tipo = "tasa" if "tasa" in texto_completo else "porcentaje"
            unidad = "%" if "tasa" in texto_completo else ""
        elif 'millones' in texto_completo:
            tipo = "millones"
            unidad = "millones"
        
        return {
            'tipo': tipo,
            'direccion': direccion,
            'unidad': unidad
        }

    def extraer_numeros_de_meta(self, meta_texto):
        """
        MEJORADO: Extrae valores de la meta CON MÁXIMA PRECISIÓN
        Maneja todos los formatos posibles
        """
        if not isinstance(meta_texto, str):
            return None, self._parse_num(meta_texto)
        
        meta_lower = meta_texto.lower()
        print(f"      🔍 Analizando meta: {meta_texto[:100]}...")
        
        # CASO 1: "de X% en el 2024 a Y% al 2029"
        # Ejemplo: "de 35,88% en el 2024 a 37,53% al 2029"
        patron_pct_con_año = re.search(
            r'de\s+([\d.,]+)\s*%\s+en\s+el\s+\d{4}\s+a\s+([\d.,]+)\s*%',
            meta_texto,
            re.IGNORECASE
        )
        if patron_pct_con_año:
            val_inicial = self._parse_num(patron_pct_con_año.group(1))
            meta_final = self._parse_num(patron_pct_con_año.group(2))
            print(f"      ✅ Extraído (% con año): Inicial={val_inicial}%, Meta={meta_final}%")
            return val_inicial, meta_final
        
        # CASO 2: "Reducir de X a Y por cada 100,000 habitantes"
        # Ejemplo: "Reducir de 12,81 a 12,25 por cada 100.000 habitantes"
        patron_tasa_100k = re.search(
            r'de\s+([\d.,]+)\s+(?:en\s+el\s+\d{4}\s+)?(?:a|al?)\s+([\d.,]+)\s+(?:por\s+cada|cada)\s+100',
            meta_texto,
            re.IGNORECASE
        )
        if patron_tasa_100k:
            val_inicial = self._parse_num(patron_tasa_100k.group(1))
            meta_final = self._parse_num(patron_tasa_100k.group(2))
            print(f"      ✅ Extraído (tasa por 100k): Inicial={val_inicial}, Meta={meta_final}")
            return val_inicial, meta_final
        
        # CASO 3: "USD X millones en el 2024 a USD Y millones al 2029"
        # Ejemplo: "USD 232,11 millones en el 2024 a USD 1.098,34 millones al 2029"
        patron_usd_con_año = re.search(
            r'USD\s+([\d.,]+)\s+millones?\s+en\s+el\s+\d{4}\s+a\s+USD\s+([\d.,]+)\s+millones?',
            meta_texto,
            re.IGNORECASE
        )
        if patron_usd_con_año:
            val_inicial = self._parse_num(patron_usd_con_año.group(1))
            meta_final = self._parse_num(patron_usd_con_año.group(2))
            print(f"      ✅ Extraído (USD millones): Inicial={val_inicial}, Meta={meta_final}")
            return val_inicial, meta_final
        
        # CASO 4: "de X% en el 2024 a Y% al 2029" (sin espacios antes de %)
        # Ejemplo: "de 83,29% en el 2024 a 90,75% al 2029"
        patron_pct_pegado = re.search(
            r'de\s+([\d.,]+)%\s+en\s+el\s+\d{4}\s+a\s+([\d.,]+)%',
            meta_texto,
            re.IGNORECASE
        )
        if patron_pct_pegado:
            val_inicial = self._parse_num(patron_pct_pegado.group(1))
            meta_final = self._parse_num(patron_pct_pegado.group(2))
            print(f"      ✅ Extraído (% pegado): Inicial={val_inicial}%, Meta={meta_final}%")
            return val_inicial, meta_final
        
        # CASO 5: "de X a Y" genérico CON FILTROS ESTRICTOS
        patron_de_a = re.search(
            r'de\s+([\d.,]+)\s*(%|millones?)?\s+(?:en\s+el\s+\d{4}\s+)?(?:a|al?)\s+([\d.,]+)\s*(%|millones?)?',
            meta_texto,
            re.IGNORECASE
        )
        if patron_de_a:
            val_inicial = self._parse_num(patron_de_a.group(1))
            meta_final = self._parse_num(patron_de_a.group(3))
            
            # FILTRO: Rechazar valores que son años o unidades
            if val_inicial in [100, 1000, 10000, 100000, 2024, 2025, 2026, 2027, 2028, 2029]:
                print(f"      ⚠️ Valor inicial {val_inicial} parece año/unidad, buscando alternativa...")
            elif meta_final in [100, 1000, 10000, 100000, 2024, 2025, 2026, 2027, 2028, 2029]:
                print(f"      ⚠️ Meta final {meta_final} parece año/unidad, buscando alternativa...")
            else:
                print(f"      ✅ Extraído (de-a genérico): Inicial={val_inicial}, Meta={meta_final}")
                return val_inicial, meta_final
        
        # CASO 6: USD millones (sin años explícitos)
        numeros_usd = re.findall(r'USD\s*([\d.,]+)\s*millones?', meta_texto, re.IGNORECASE)
        if len(numeros_usd) >= 2:
            val_ini = self._parse_num(numeros_usd[0])
            val_fin = self._parse_num(numeros_usd[1])
            print(f"      ✅ Extraído (USD simple): Inicial={val_ini}, Meta={val_fin}")
            return val_ini, val_fin
        elif len(numeros_usd) == 1:
            val = self._parse_num(numeros_usd[0])
            print(f"      ⚠️ Solo una cifra USD encontrada: Meta={val}")
            return None, val
        
        # CASO 7: Porcentajes simples
        numeros_pct = re.findall(r'([\d.,]+)\s*%', meta_texto)
        if len(numeros_pct) >= 2:
            val_ini = self._parse_num(numeros_pct[0])
            val_fin = self._parse_num(numeros_pct[-1])  # Último porcentaje
            # Filtrar 100% que suele ser "100.000 habitantes"
            if val_ini not in [100] and val_fin not in [100]:
                print(f"      ✅ Extraído (% simple): Inicial={val_ini}%, Meta={val_fin}%")
                return val_ini, val_fin
        
        # CASO 8: FALLBACK - Números limpios (MUY CONSERVADOR)
        numeros_general = re.findall(r'(\d+[.,]\d+)', meta_texto)  # Solo decimales
        numeros_limpios = [
            self._parse_num(n) for n in numeros_general 
            if self._parse_num(n) not in [100, 1000, 10000, 100000] and 
               not (2020 <= self._parse_num(n) <= 2030)  # No años
        ]
        
        if len(numeros_limpios) >= 2:
            print(f"      ⚠️ Usando fallback: Inicial={numeros_limpios[0]}, Meta={numeros_limpios[-1]}")
            return numeros_limpios[0], numeros_limpios[-1]
        
        print(f"      ❌ NO SE PUDO EXTRAER de: {meta_texto}")
        return None, None

    def calcular_progreso_inteligente(self, val_inicial, val_actual, meta_final, direccion):
        """Calcula progreso con validación robusta"""
        if val_actual is None or val_inicial is None or meta_final is None:
            return 0
        
        # VALIDACIÓN: Detectar incoherencias de magnitud
        if meta_final > 0:
            ratio = val_actual / meta_final
            if ratio > 50:  # Valor actual 50x más grande que meta
                print(f"      ⚠️ INCOHERENCIA: Actual {val_actual} vs Meta {meta_final}")
                if val_actual > 1000:
                    val_actual = val_actual / 1000
                    print(f"      🔧 Corrigiendo: {val_actual}")
                elif val_actual > 100:
                    val_actual = val_actual / 100
                    print(f"      🔧 Corrigiendo: {val_actual}")
        
        try:
            if direccion == "reducir":
                rango_total = val_inicial - meta_final
                avance_logrado = val_inicial - val_actual
                
                if rango_total <= 0: return 0
                progreso = (avance_logrado / rango_total) * 100
                
            else:  # incrementar
                rango_total = meta_final - val_inicial
                avance_logrado = val_actual - val_inicial
                
                if rango_total == 0:
                    return 100.0 if val_actual >= meta_final else 0
                
                progreso = (avance_logrado / rango_total) * 100
            
            return round(max(0, min(150, progreso)), 2)
            
        except Exception as e:
            print(f"      ❌ Error calculando progreso: {e}")
            return 0

    def extraer_texto_fuentes(self, datos_scraping):
        """Extrae contexto de las fuentes para análisis"""
        bloques = []
        for item in datos_scraping:
            for d in item.get("numeros_contexto", [])[:3]:
                if d.get("contexto"):
                    bloques.append(d["contexto"])
        return "\n".join(bloques)

    def analizar_indicador(self, eje, indicador, meta, valor_inicial, valor_actual, datos_scraping, contexto):
        """Análisis completo del indicador"""
        # 1. Analizar tipo
        info_indicador = self.analizar_tipo_indicador(indicador, meta)
        
        # 2. Extraer valores de la meta
        val_ini_meta, meta_num = self.extraer_numeros_de_meta(meta)
        
        # 3. Usar valor inicial proporcionado o extraído
        val_ini = self._parse_num(valor_inicial) if valor_inicial is not None else val_ini_meta
        val_act = self._parse_num(valor_actual)

        # 4. Calcular progreso
        progreso = self.calcular_progreso_inteligente(
            val_ini, 
            val_act, 
            meta_num, 
            info_indicador['direccion']
        )

        # 5. Determinar estado
        if progreso >= 75:
            estado = "eficiente"
            eficiencia = "Alta"
        elif progreso >= 50:
            estado = "moderado"
            eficiencia = "Media-Alta"
        elif progreso >= 30:
            estado = "moderado"
            eficiencia = "Media"
        elif progreso >= 15:
            estado = "bajo"
            eficiencia = "Media-Baja"
        else:
            estado = "deficiente"
            eficiencia = "Baja"

        # 6. Extraer fecha
        fecha = "No disponible"
        for r in datos_scraping:
            fechas = r.get("fechas_encontradas", [])
            if fechas:
                fecha = fechas[0]
                break

        texto_fuentes = self.extraer_texto_fuentes(datos_scraping)
        
        # Debug
        print(f"\n{'='*60}")
        print(f"📊 ANÁLISIS FINAL")
        print(f"{'='*60}")
        print(f"Indicador: {indicador}")
        print(f"Tipo: {info_indicador['tipo']} | Dirección: {info_indicador['direccion']}")
        print(f"Valor Inicial: {val_ini} {info_indicador['unidad']}")
        print(f"Meta Final: {meta_num} {info_indicador['unidad']}")
        print(f"Valor Actual: {val_act} {info_indicador['unidad']}")
        print(f"Progreso: {progreso}% ({estado})")
        print(f"{'='*60}\n")
        
        # 7. Análisis con IA
        if val_act is None or val_ini is None or meta_num is None:
            analisis = "No se pudo realizar el análisis debido a falta de datos. Se requiere información actualizada del indicador."
        else:
            verbo = "reducir" if info_indicador['direccion'] == "reducir" else "incrementar"
            
            prompt = f"""
Analiza este indicador de política pública ecuatoriana:

DATOS:
- Indicador: {indicador}
- Objetivo: {verbo.upper()} de {val_ini} a {meta_num} {info_indicador['unidad']}
- Valor actual: {val_act} {info_indicador['unidad']}
- Progreso: {progreso}%

CONTEXTO DE FUENTES:
{texto_fuentes[:3000]}

Genera un análisis en EXACTAMENTE 4 oraciones:
1. Compara valor actual vs inicial
2. Evalúa si el progreso es suficiente
3. Identifica el principal riesgo
4. Da una recomendación específica

Sin viñetas, solo texto corrido.
"""
            
            analisis = "No disponible."
            try:
                if self.verificar_ollama():
                    resp = ollama.chat(
                        model=self.model,
                        messages=[
                            {"role": "system", "content": "Analista técnico de políticas públicas. Conciso y objetivo."},
                            {"role": "user", "content": prompt}
                        ]
                    )
                    analisis = resp.get("message", {}).get("content", "").strip()
            except Exception as e:
                print(f"⚠️ Error Ollama: {e}")
                # Análisis de respaldo
                if info_indicador['direccion'] == "reducir":
                    if val_act is not None and val_ini is not None:
                        if val_act < val_ini:
                            analisis = f"El indicador muestra una reducción de {val_ini} a {val_act}{info_indicador['unidad']}, avanzando hacia la meta. Con un progreso del {progreso}%, se requiere mantener políticas activas. El riesgo es perder momentum. Se recomienda monitoreo trimestral y ajustes según necesidad."
                        else:
                            analisis = f"El indicador aumentó de {val_ini} a {val_act}{info_indicador['unidad']}, contradiciendo el objetivo de reducción. El progreso del {progreso}% refleja retroceso. Riesgo crítico de no alcanzar meta. Se requiere revisión urgente de estrategias."
                else:
                    if val_act is not None and val_ini is not None:
                        if val_act > val_ini:
                            analisis = f"El indicador creció de {val_ini} a {val_act}{info_indicador['unidad']}, mostrando avance positivo. El progreso del {progreso}% indica necesidad de acelerar. Riesgo de no sostener crecimiento. Se recomienda continuar políticas actuales con optimizaciones."
                        else:
                            analisis = f"El indicador disminuyó de {val_ini} a {val_act}{info_indicador['unidad']}, contradiciendo el objetivo de incremento. Progreso del {progreso}% indica retroceso. Riesgo grave de alejarse de meta. Requiere redefinición inmediata de estrategias."

        return {
            "valor_inicial": val_ini if val_ini is not None else "No disponible",
            "valor_actual": val_act if val_act is not None else "No disponible",
            "fecha_actualizacion": fecha,
            "progreso": progreso,
            "estado": estado,
            "eficiencia": eficiencia,
            "analisis": analisis,
            "tipo_indicador": info_indicador['tipo'],
            "direccion": info_indicador['direccion'],
            "unidad": info_indicador['unidad'],
            "fuente": datos_scraping[0].get("fuente") if datos_scraping else "No disponible"
        }