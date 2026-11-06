# 🦈 ORCA SHARK QUANTUM — PROJETO 1 (PARTE PÚBLICA: BOT PREDICTOR 25-AGENTS & SPREAD NA WEB)

Este é o **Projeto 1 (Parte Pública)** da arquitetura dividida em dois projetos:
- **Foco**: Toda a estrutura analítica do **Bot Predictor GOD (Enxame de 25 Agentes Superinteligência `S01`–`S25`)**, **Análise de Spread na Web em Tempo Real**, **Cone Preditivo (`+15s / +30s`)**, **Sinal Consolidado `ENTER $+`** e **Interface High-Aesthetic** para o operador humano analisar as informações e executar entradas rápidas de `$1.00`, `$2.00`, `$3.00`, `$5.00` (cotas quebradinhas igual ao front-end da Polymarket) e Cash Out de 1-clique.
- **Zero Credenciais no Código-Fonte**: 100% seguro para distribuição ou exibição pública.

---

## 🚀 Como Iniciar o Painel Público (`http://127.0.0.1:8787`)

```bash
cd /home/god/.gemini/antigravity/scratch/polymarket_predictor_public
cargo run --release --bin predictor_web -- 8787
# Acesse no navegador: http://127.0.0.1:8787
```

---

## ✨ Destaques da Arquitetura Pública (`predictor_web`)
1. **Design High-Aesthetic ("Obsidian Quantum Glass")**:
   - Superfícies em vidro obsidiana com iluminação radial atmosférica, tipografia tabular de alta precisão e zero lag de GPU.
2. **Faixa de Spread na Web & Microestrutura CLOB (`📊 SPREAD NA WEB`)**:
   - Exibe ao vivo o `Spread UP (¢)`, `Spread DOWN (¢)`, `Soma Ask UP+DN ($)` (identificando descontos de arbitragem quando `< $1.000`) e a barra de **Pressão de Liquidez do Livro (`% UP vs % DN`)**.
3. **Enxame de 25 Agentes Superinteligência (`S01`–`S25` com Pesos Bayesianos EXP3)**:
   - `S01`–`S20` (Sensores de Micro-Alpha, Baleias, VPIN, Navier-Stokes, Kelly, RLS +15s/+30s, OU, Markov) + `S21` Fractal Hurst + `S22` MM Spread Skew + `S23` Brownian Bridge + `S24` Phase Coherence + `S25` Meta-Oracle Bayesian Hedge.
4. **Entradas de `$1`, `$2`, `$3` (Cotas Quebradinhas) & Visualização de Lucro de Entrada para Sacar**:
   - Botões rápidos idênticos ao front-end da Polymarket + cálculo em tempo real (`350ms`) do valor de saque e lucro líquido.
