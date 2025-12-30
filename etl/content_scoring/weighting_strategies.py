"""
Weighting Strategies for Editorial Ranking
===========================================

Design Patterns Implemented:
1. Strategy Pattern: Encapsulates different weighting algorithms
2. Factory Pattern: Creates appropriate strategy instances
3. Template Method Pattern: Defines skeleton of weighting algorithm

This allows easy extension and modification of weighting logic without
changing the core calculator code.
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional
from dataclasses import dataclass
import logging


logger = logging.getLogger(__name__)


# =============================================================================
# Strategy Pattern: WeightingStrategy Interface
# =============================================================================

class WeightingStrategy(ABC):
    """
    Abstract base class for weighting strategies (Strategy Pattern).
    
    Each concrete strategy implements different weighting logic for
    combining reach, engagement, and depth features into a final score.
    """
    
    @abstractmethod
    def get_weights(self) -> Dict[str, float]:
        """
        Return feature weights as a dictionary.
        
        Returns:
            Dict mapping feature names to weights (must sum to 1.0)
        """
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        """Return human-readable strategy name."""
        pass
    
    @abstractmethod
    def get_description(self) -> str:
        """Return strategy description and use case."""
        pass
    
    def validate_weights(self, weights: Dict[str, float]) -> None:
        """
        Validate that weights sum to 1.0 (Template Method Pattern).
        
        Args:
            weights: Dictionary of feature weights
            
        Raises:
            ValueError: If weights don't sum to 1.0
        """
        total = sum(weights.values())
        if not (0.99 <= total <= 1.01):
            raise ValueError(
                f"Weights must sum to 1.0. Got {total} for {self.get_name()}"
            )
    
    def apply_weights(self, feature_ranks: Dict[str, float]) -> float:
        """
        Apply weights to feature ranks to calculate final score (Template Method).
        
        Args:
            feature_ranks: Dict mapping feature names to their ranks [0, 1]
            
        Returns:
            Weighted score [0, 1]
        """
        weights = self.get_weights()
        
        # Validate that all required features are present
        required = set(weights.keys())
        available = set(feature_ranks.keys())
        
        if not required.issubset(available):
            missing = required - available
            raise ValueError(f"Missing features for {self.get_name()}: {missing}")
        
        # Calculate weighted sum
        score = sum(
            feature_ranks[feature] * weight 
            for feature, weight in weights.items()
        )
        
        return score


# =============================================================================
# Concrete Strategies
# =============================================================================

class BalancedStrategy(WeightingStrategy):
    """
    Balanced weighting strategy (default).
    
    Equal emphasis on reach and engagement, slightly less on depth.
    Good for general editorial content.
    """
    
    def get_weights(self) -> Dict[str, float]:
        return {
            'reach': 0.35,
            'engagement': 0.35,
            'depth': 0.30
        }
    
    def get_name(self) -> str:
        return "Balanced"
    
    def get_description(self) -> str:
        return (
            "Balanced strategy for general editorial content. "
            "Equal emphasis on reach and engagement (35% each), "
            "with depth at 30%. Suitable for mixed content portfolios."
        )


class QualityFocusedStrategy(WeightingStrategy):
    """
    Quality-focused strategy.
    
    Emphasizes engagement and depth over raw reach.
    Best for premium content, in-depth analysis, and brand reputation.
    """
    
    def get_weights(self) -> Dict[str, float]:
        return {
            'reach': 0.20,
            'engagement': 0.45,
            'depth': 0.35
        }
    
    def get_name(self) -> str:
        return "Quality-Focused"
    
    def get_description(self) -> str:
        return (
            "Quality-focused strategy for premium content. "
            "Emphasizes engagement (45%) and depth (35%) over reach (20%). "
            "Best for: in-depth reviews, analysis, brand-building content."
        )


class VolumeFocusedStrategy(WeightingStrategy):
    """
    Volume-focused strategy.
    
    Emphasizes reach over quality metrics.
    Best for news, breaking content, and traffic-driven objectives.
    """
    
    def get_weights(self) -> Dict[str, float]:
        return {
            'reach': 0.55,
            'engagement': 0.25,
            'depth': 0.20
        }
    
    def get_name(self) -> str:
        return "Volume-Focused"
    
    def get_description(self) -> str:
        return (
            "Volume-focused strategy for traffic objectives. "
            "Emphasizes reach (55%) over engagement (25%) and depth (20%). "
            "Best for: breaking news, trending topics, SEO-driven content."
        )


class EngagementDrivenStrategy(WeightingStrategy):
    """
    Engagement-driven strategy.
    
    Maximum emphasis on user engagement.
    Best for social media content, viral potential, community building.
    """
    
    def get_weights(self) -> Dict[str, float]:
        return {
            'reach': 0.25,
            'engagement': 0.55,
            'depth': 0.20
        }
    
    def get_name(self) -> str:
        return "Engagement-Driven"
    
    def get_description(self) -> str:
        return (
            "Engagement-driven strategy for viral potential. "
            "Maximum emphasis on engagement (55%), moderate reach (25%), "
            "lower depth (20%). Best for: social content, interactive pieces."
        )


class DeepDiveStrategy(WeightingStrategy):
    """
    Deep-dive strategy.
    
    Emphasizes depth and engagement for long-form content.
    Best for investigative journalism, documentaries, educational content.
    """
    
    def get_weights(self) -> Dict[str, float]:
        return {
            'reach': 0.20,
            'engagement': 0.35,
            'depth': 0.45
        }
    
    def get_name(self) -> str:
        return "Deep-Dive"
    
    def get_description(self) -> str:
        return (
            "Deep-dive strategy for long-form content. "
            "Maximum emphasis on depth (45%), good engagement (35%), "
            "lower reach (20%). Best for: investigations, documentaries, essays."
        )


class ViralOptimizedStrategy(WeightingStrategy):
    """
    Viral-optimized strategy.
    
    Optimizes for content with viral potential.
    High reach and engagement, less concern for depth.
    """
    
    def get_weights(self) -> Dict[str, float]:
        return {
            'reach': 0.45,
            'engagement': 0.45,
            'depth': 0.10
        }
    
    def get_name(self) -> str:
        return "Viral-Optimized"
    
    def get_description(self) -> str:
        return (
            "Viral-optimized strategy for shareability. "
            "Equal emphasis on reach and engagement (45% each), "
            "minimal depth (10%). Best for: memes, lists, quick reads."
        )


class CustomStrategy(WeightingStrategy):
    """
    Custom strategy with user-defined weights.
    
    Allows complete control over weighting distribution.
    """
    
    def __init__(
        self,
        reach_weight: float,
        engagement_weight: float,
        depth_weight: float,
        name: str = "Custom",
        description: Optional[str] = None
    ):
        """
        Initialize custom strategy.
        
        Args:
            reach_weight: Weight for reach feature
            engagement_weight: Weight for engagement feature
            depth_weight: Weight for depth feature
            name: Strategy name
            description: Strategy description
        """
        self._weights = {
            'reach': reach_weight,
            'engagement': engagement_weight,
            'depth': depth_weight
        }
        self._name = name
        self._description = description or f"Custom strategy: {self._weights}"
        
        # Validate on initialization
        self.validate_weights(self._weights)
    
    def get_weights(self) -> Dict[str, float]:
        return self._weights.copy()
    
    def get_name(self) -> str:
        return self._name
    
    def get_description(self) -> str:
        return self._description


# =============================================================================
# Factory Pattern: WeightingStrategyFactory
# =============================================================================

class WeightingStrategyFactory:
    """
    Factory for creating weighting strategies (Factory Pattern).
    
    Centralizes strategy creation and provides easy access to all
    available strategies.
    """
    
    # Registry of available strategies
    _strategies = {
        'balanced': BalancedStrategy,
        'quality': QualityFocusedStrategy,
        'volume': VolumeFocusedStrategy,
        'engagement': EngagementDrivenStrategy,
        'deep-dive': DeepDiveStrategy,
        'viral': ViralOptimizedStrategy,
    }
    
    @classmethod
    def create(cls, strategy_name: str, **kwargs) -> WeightingStrategy:
        """
        Create a weighting strategy by name (Factory Method).
        
        Args:
            strategy_name: Name of strategy to create
            **kwargs: Additional arguments for custom strategies
            
        Returns:
            WeightingStrategy instance
            
        Raises:
            ValueError: If strategy name is unknown
            
        Examples:
            >>> factory = WeightingStrategyFactory()
            >>> strategy = factory.create('balanced')
            >>> strategy = factory.create('custom', reach_weight=0.4, 
            ...                          engagement_weight=0.4, depth_weight=0.2)
        """
        strategy_name = strategy_name.lower()
        
        # Handle custom strategy
        if strategy_name == 'custom':
            required = ['reach_weight', 'engagement_weight', 'depth_weight']
            if not all(k in kwargs for k in required):
                raise ValueError(
                    f"Custom strategy requires: {required}"
                )
            return CustomStrategy(**kwargs)
        
        # Handle predefined strategies
        if strategy_name not in cls._strategies:
            available = list(cls._strategies.keys()) + ['custom']
            raise ValueError(
                f"Unknown strategy '{strategy_name}'. "
                f"Available: {available}"
            )
        
        strategy_class = cls._strategies[strategy_name]
        return strategy_class()
    
    @classmethod
    def get_available_strategies(cls) -> Dict[str, str]:
        """
        Get all available strategies with descriptions.
        
        Returns:
            Dict mapping strategy names to descriptions
        """
        strategies = {}
        for name, strategy_class in cls._strategies.items():
            instance = strategy_class()
            strategies[name] = instance.get_description()
        
        strategies['custom'] = "Custom strategy with user-defined weights"
        
        return strategies
    
    @classmethod
    def list_strategies(cls) -> None:
        """Print all available strategies with descriptions."""
        print("\n📊 Available Weighting Strategies:\n")
        
        for name, description in cls.get_available_strategies().items():
            strategy = cls.create(name) if name != 'custom' else None
            
            print(f"🔹 {name.upper()}")
            if strategy:
                weights = strategy.get_weights()
                print(f"   Weights: Reach={weights['reach']:.0%}, "
                      f"Engagement={weights['engagement']:.0%}, "
                      f"Depth={weights['depth']:.0%}")
            print(f"   {description}\n")


# =============================================================================
# Convenience Functions
# =============================================================================

def create_strategy(strategy_name: str, **kwargs) -> WeightingStrategy:
    """
    Convenience function to create a strategy.
    
    Args:
        strategy_name: Strategy name
        **kwargs: Additional arguments for custom strategies
        
    Returns:
        WeightingStrategy instance
    """
    return WeightingStrategyFactory.create(strategy_name, **kwargs)


def list_available_strategies() -> None:
    """List all available strategies."""
    WeightingStrategyFactory.list_strategies()


# =============================================================================
# Usage Examples (for documentation)
# =============================================================================

if __name__ == "__main__":
    # Example 1: Create predefined strategy
    balanced = create_strategy('balanced')
    print(f"Strategy: {balanced.get_name()}")
    print(f"Weights: {balanced.get_weights()}")
    
    # Example 2: Create custom strategy
    custom = create_strategy(
        'custom',
        reach_weight=0.4,
        engagement_weight=0.4,
        depth_weight=0.2,
        name="My Custom Strategy"
    )
    print(f"\nStrategy: {custom.get_name()}")
    print(f"Weights: {custom.get_weights()}")
    
    # Example 3: List all strategies
    list_available_strategies()
    
    # Example 4: Apply weights to features
    feature_ranks = {
        'reach': 0.75,
        'engagement': 0.60,
        'depth': 0.80
    }
    
    quality_strategy = create_strategy('quality')
    score = quality_strategy.apply_weights(feature_ranks)
    print(f"\nApplied {quality_strategy.get_name()} strategy:")
    print(f"Score: {score:.3f}")
