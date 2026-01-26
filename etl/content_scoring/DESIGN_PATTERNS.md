# Design Patterns Implementation
## Strategy Pattern + Factory Pattern

### 📋 Overview

Il sistema di content scoring implementa due design patterns fondamentali:

1. **Strategy Pattern** - Permette di selezionare algoritmi di weighting intercambiabili a runtime
2. **Factory Pattern** - Centralizza la creazione delle strategie

### 🎯 Perché questi Design Patterns?

#### **Strategy Pattern**
**Problema Risolto**: Il sistema originale aveva pesi hardcoded nel config, rendendo difficile:
- Testare diverse combinazioni di pesi
- Adattare il ranking a diversi obiettivi editoriali
- Estendere il sistema con nuovi algoritmi

**Soluzione**: Incapsulare ogni algoritmo di weighting in una classe separata che implementa l'interfaccia `WeightingStrategy`.

**Vantaggi**:
- ✅ **Open/Closed Principle**: Aperto all'estensione, chiuso alla modifica
- ✅ **Single Responsibility**: Ogni strategia gestisce solo la sua logica di weighting
- ✅ **Runtime Flexibility**: Cambio strategia senza ricompilare
- ✅ **Easy Testing**: Test isolati per ogni strategia

#### **Factory Pattern**
**Problema Risolto**: Creazione complessa di strategie con validazione e configurazione.

**Soluzione**: Centralizzare la logica di creazione in `WeightingStrategyFactory`.

**Vantaggi**:
- ✅ **Encapsulation**: Logica di creazione nascosta
- ✅ **Consistency**: Validazione centralizzata
- ✅ **Discovery**: Listing automatico di tutte le strategie disponibili

---

## 🏗️ Architettura

### Class Diagram

```
┌─────────────────────────────┐
│   WeightingStrategy         │ ◄──── Abstract Base Class (Strategy Pattern)
│   (ABC)                     │
├─────────────────────────────┤
│ + get_weights()             │
│ + get_name()                │
│ + get_description()         │
│ + apply_weights()           │
│ # validate_weights()        │ (Template Method Pattern)
└─────────────────────────────┘
              △
              │ implements
              │
    ┌─────────┼──────────┬───────────┬──────────────┬────────────┐
    │         │          │           │              │            │
┌───────┐ ┌────────┐ ┌─────────┐ ┌──────────┐ ┌─────────┐ ┌─────────┐
│Balanced│ │Quality │ │ Volume  │ │Engagement│ │Deep-Dive│ │  Viral  │
│Strategy│ │Focused │ │ Focused │ │ Driven   │ │Strategy │ │Optimized│
└───────┘ └────────┘ └─────────┘ └──────────┘ └─────────┘ └─────────┘

┌──────────────────────────────┐
│  WeightingStrategyFactory    │ ◄──── Factory Pattern
├──────────────────────────────┤
│ + create(name, **kwargs)     │
│ + get_available_strategies() │
│ + list_strategies()          │
└──────────────────────────────┘
              │
              │ creates
              ▼
    WeightingStrategy instances

┌──────────────────────────────┐
│  ContentScoreCalculator      │ ◄──── Context (uses Strategy)
├──────────────────────────────┤
│ - strategy: WeightingStrategy│ ◄──── Composition
│ - config: Config             │
│ + calculate()                │
│ - _initialize_strategy()     │ ◄──── Uses Factory
└──────────────────────────────┘
```

### Sequence Diagram

```
User                Calculator          Factory           Strategy
 │                      │                  │                 │
 │ create(config)       │                  │                 │
 ├─────────────────────►│                  │                 │
 │                      │                  │                 │
 │                      │ create(name)     │                 │
 │                      ├─────────────────►│                 │
 │                      │                  │                 │
 │                      │                  │ new Strategy()  │
 │                      │                  ├────────────────►│
 │                      │                  │                 │
 │                      │  strategy        │                 │
 │                      │◄─────────────────┤                 │
 │                      │                  │                 │
 │ calculate(data)      │                  │                 │
 ├─────────────────────►│                  │                 │
 │                      │                  │                 │
 │                      │ apply_weights(features)            │
 │                      ├────────────────────────────────────►│
 │                      │                  │                 │
 │                      │         weighted_score             │
 │                      │◄────────────────────────────────────┤
 │                      │                  │                 │
 │      result          │                  │                 │
 │◄─────────────────────┤                  │                 │
```

---

## 🔧 Implementazione

### 1. Strategy Pattern

#### Interface: `WeightingStrategy` (ABC)

```python
class WeightingStrategy(ABC):
    """Abstract strategy for weighting algorithms."""
    
    @abstractmethod
    def get_weights(self) -> Dict[str, float]:
        """Return feature weights."""
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """Return strategy name."""
        pass
    
    def apply_weights(self, feature_ranks: Dict[str, float]) -> float:
        """Template Method: Apply weights to features."""
        weights = self.get_weights()
        return sum(feature_ranks[f] * w for f, w in weights.items())
```

#### Concrete Strategies

**BalancedStrategy** (Default)
```python
class BalancedStrategy(WeightingStrategy):
    def get_weights(self):
        return {'reach': 0.35, 'engagement': 0.35, 'depth': 0.30}
```

**QualityFocusedStrategy** (Premium Content)
```python
class QualityFocusedStrategy(WeightingStrategy):
    def get_weights(self):
        return {'reach': 0.20, 'engagement': 0.45, 'depth': 0.35}
```

**VolumeFocusedStrategy** (Traffic Objectives)
```python
class VolumeFocusedStrategy(WeightingStrategy):
    def get_weights(self):
        return {'reach': 0.55, 'engagement': 0.25, 'depth': 0.20}
```

**CustomStrategy** (User-Defined)
```python
class CustomStrategy(WeightingStrategy):
    def __init__(self, reach_weight, engagement_weight, depth_weight):
        self._weights = {...}
        self.validate_weights(self._weights)
```

### 2. Factory Pattern

```python
class WeightingStrategyFactory:
    """Factory for creating strategies."""
    
    _strategies = {
        'balanced': BalancedStrategy,
        'quality': QualityFocusedStrategy,
        'volume': VolumeFocusedStrategy,
        # ... more strategies
    }
    
    @classmethod
    def create(cls, strategy_name: str, **kwargs) -> WeightingStrategy:
        """Create strategy by name."""
        if strategy_name == 'custom':
            return CustomStrategy(**kwargs)
        
        strategy_class = cls._strategies[strategy_name]
        return strategy_class()
```

### 3. Integration with Calculator

```python
class ContentScoreCalculator:
    def __init__(self, config: ContentScoringConfig):
        self.config = config
        self._initialize_strategy()  # Uses Factory
    
    def _initialize_strategy(self):
        """Initialize strategy using Factory Pattern."""
        if self.config.strategy_name == 'custom':
            self.strategy = WeightingStrategyFactory.create(
                'custom',
                reach_weight=self.config.reach_weight,
                engagement_weight=self.config.engagement_weight,
                depth_weight=self.config.depth_weight
            )
        else:
            self.strategy = WeightingStrategyFactory.create(
                self.config.strategy_name
            )
    
    def _calculate_editorial_score(self, df):
        """Use strategy to calculate scores."""
        def apply_strategy(row):
            feature_ranks = {
                'reach': row['feature_reach_rank'],
                'engagement': row['feature_engagement_rank'],
                'depth': row['feature_depth_rank']
            }
            return self.strategy.apply_weights(feature_ranks)
        
        df['editorial_score'] = df.apply(apply_strategy, axis=1) * 100
```

---

## 📚 Usage Examples

### Example 1: Use Predefined Strategy

```python
from etl.content_scoring import ContentScoreCalculator, ContentScoringConfig

# Quality-focused strategy
config = ContentScoringConfig(strategy_name='quality')
calculator = ContentScoreCalculator(config)
result = calculator.calculate(df)
```

### Example 2: Custom Strategy

```python
config = ContentScoringConfig(
    strategy_name='custom',
    reach_weight=0.40,
    engagement_weight=0.40,
    depth_weight=0.20
)
calculator = ContentScoreCalculator(config)
result = calculator.calculate(df)
```

### Example 3: Runtime Strategy Switching

```python
calculator = ContentScoreCalculator(
    ContentScoringConfig(strategy_name='balanced')
)

# Calculate with balanced
result1 = calculator.calculate(df)

# Switch to quality strategy
calculator.config.strategy_name = 'quality'
calculator._initialize_strategy()

# Calculate with quality
result2 = calculator.calculate(df)
```

### Example 4: List Available Strategies

```python
from etl.content_scoring import list_available_strategies

list_available_strategies()
```

### Example 5: Compare Strategies

```python
strategies = ['balanced', 'quality', 'volume']
results = {}

for strategy_name in strategies:
    config = ContentScoringConfig(strategy_name=strategy_name)
    calculator = ContentScoreCalculator(config)
    results[strategy_name] = calculator.calculate(df)

# Compare rankings
for name, result in results.items():
    print(f"{name}: {result['editorial_rank'].tolist()}")
```

---

## 🎨 Design Pattern Benefits

### Before (Hardcoded Weights)
```python
# ❌ Problem: Weights hardcoded in config
class ContentScoringConfig:
    reach_weight: float = 0.35
    engagement_weight: float = 0.35
    depth_weight: float = 0.30

# ❌ Problem: Calculator tightly coupled to specific weights
def _calculate_score(df):
    score = (
        df['reach_rank'] * config.reach_weight +
        df['engagement_rank'] * config.engagement_weight +
        df['depth_rank'] * config.depth_weight
    )
```

**Issues**:
- 🔴 Adding new weighting algorithm requires modifying calculator
- 🔴 Testing different weight combinations is cumbersome
- 🔴 No clear separation of concerns
- 🔴 Violates Open/Closed Principle

### After (Strategy Pattern)
```python
# ✅ Solution: Strategy interface
class WeightingStrategy(ABC):
    @abstractmethod
    def apply_weights(self, features: Dict) -> float:
        pass

# ✅ Solution: Concrete strategies
class BalancedStrategy(WeightingStrategy):
    def apply_weights(self, features):
        return features['reach'] * 0.35 + ...

class QualityStrategy(WeightingStrategy):
    def apply_weights(self, features):
        return features['reach'] * 0.20 + ...

# ✅ Solution: Calculator uses strategy
def _calculate_score(df):
    score = df.apply(
        lambda row: self.strategy.apply_weights({
            'reach': row['reach_rank'],
            'engagement': row['engagement_rank'],
            'depth': row['depth_rank']
        }),
        axis=1
    )
```

**Benefits**:
- ✅ Adding new strategy = new class (no modification to calculator)
- ✅ Easy to test each strategy independently
- ✅ Clear separation of concerns
- ✅ Follows Open/Closed Principle

---

## 🧪 Testing Strategies

### Unit Test Example

```python
def test_quality_strategy():
    """Test quality-focused strategy weights."""
    strategy = QualityFocusedStrategy()
    
    # Verify weights
    weights = strategy.get_weights()
    assert weights['reach'] == 0.20
    assert weights['engagement'] == 0.45
    assert weights['depth'] == 0.35
    
    # Verify they sum to 1.0
    assert sum(weights.values()) == 1.0
    
    # Test application
    features = {'reach': 0.5, 'engagement': 0.8, 'depth': 0.6}
    score = strategy.apply_weights(features)
    
    expected = 0.5 * 0.20 + 0.8 * 0.45 + 0.6 * 0.35
    assert abs(score - expected) < 0.001
```

### Integration Test Example

```python
def test_strategy_switching():
    """Test runtime strategy switching."""
    df = create_test_dataframe()
    
    # Test with balanced
    calculator = ContentScoreCalculator(
        ContentScoringConfig(strategy_name='balanced')
    )
    result1 = calculator.calculate(df)
    
    # Switch to quality
    calculator.config.strategy_name = 'quality'
    calculator._initialize_strategy()
    result2 = calculator.calculate(df)
    
    # Verify different results
    assert not result1['editorial_score'].equals(result2['editorial_score'])
```

---

## 📊 Available Strategies

| Strategy | Reach | Engagement | Depth | Best For |
|----------|-------|------------|-------|----------|
| **Balanced** | 35% | 35% | 30% | General editorial content |
| **Quality-Focused** | 20% | 45% | 35% | Premium content, analysis |
| **Volume-Focused** | 55% | 25% | 20% | News, SEO content |
| **Engagement-Driven** | 25% | 55% | 20% | Social media, viral |
| **Deep-Dive** | 20% | 35% | 45% | Long-form, investigations |
| **Viral-Optimized** | 45% | 45% | 10% | Quick reads, lists |
| **Custom** | X% | Y% | Z% | User-defined weights |

---

## 🔮 Future Extensions

### Easy to Add New Strategies

```python
class SeasonalStrategy(WeightingStrategy):
    """Strategy that adapts to seasonal patterns."""
    
    def __init__(self, season: str):
        self.season = season
    
    def get_weights(self):
        if self.season == 'summer':
            return {'reach': 0.50, 'engagement': 0.30, 'depth': 0.20}
        elif self.season == 'winter':
            return {'reach': 0.25, 'engagement': 0.40, 'depth': 0.35}
```

### Machine Learning Strategy

```python
class MLStrategy(WeightingStrategy):
    """Strategy with ML-optimized weights."""
    
    def __init__(self, model_path: str):
        self.model = load_model(model_path)
    
    def apply_weights(self, features):
        # Use ML model to predict optimal weight combination
        return self.model.predict([features])[0]
```

### A/B Testing Strategy

```python
class ABTestStrategy(WeightingStrategy):
    """Strategy for A/B testing different weight combinations."""
    
    def __init__(self, variant: str):
        self.variant = variant
        self.variants = {
            'A': {'reach': 0.33, 'engagement': 0.33, 'depth': 0.34},
            'B': {'reach': 0.40, 'engagement': 0.35, 'depth': 0.25},
        }
    
    def get_weights(self):
        return self.variants[self.variant]
```

---

## ✅ Summary

### Design Patterns Used

1. **Strategy Pattern** (`WeightingStrategy`)
   - Encapsulates weighting algorithms
   - Allows runtime selection
   - Promotes Open/Closed Principle

2. **Factory Pattern** (`WeightingStrategyFactory`)
   - Centralizes strategy creation
   - Validates configuration
   - Provides discovery mechanism

3. **Template Method Pattern** (in `apply_weights`)
   - Defines algorithm skeleton
   - Allows subclass customization

### Key Benefits

- ✅ **Flexibility**: Easy to add new weighting strategies
- ✅ **Testability**: Each strategy can be tested independently
- ✅ **Maintainability**: Clear separation of concerns
- ✅ **Scalability**: System grows without modifying existing code
- ✅ **Reusability**: Strategies can be reused across different contexts

### Code Quality Metrics

- **Cyclomatic Complexity**: Low (each strategy is simple)
- **Coupling**: Low (strategies are independent)
- **Cohesion**: High (each strategy has single responsibility)
- **Extensibility**: High (easy to add new strategies)
