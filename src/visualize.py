import matplotlib.pyplot as plt
import seaborn as sns
# import pandas as pd
import os
from src.data_loader import create_analysis_dataset


def plot_war_vs_salary(season: int = 2024):
    """
    Generates a scatter plot of WAR vs Salary for the Blue Jays.
    Saves the output to 'plots/war_vs_salary.png'.
    """
    # 1. Get Data
    df = create_analysis_dataset(season)
    if df.empty:
        print("No data available to plot.")
        return

    # 2. Setup Plot Style
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))

    # 3. Create Scatter Plot
    # Convert Salary to Millions for readability
    df['Salary_M'] = df['Salary'] / 1_000_000
    sns.scatterplot(
        data=df,
        x='WAR',
        y='Salary_M',
        s=100,
        color='#134A8E'  # Blue Jays Blue
    )

    # 4. Add Labels (Names) to points
    for i, row in df.iterrows():
        plt.text(
            row['WAR'] + 0.1,
            row['Salary_M'],
            row['Name'],
            fontsize=9
        )

    plt.title(f"Blue Jays {season}: WAR vs Salary (Million USD)", fontsize=16)
    plt.xlabel("WAR (Wins Above Replacement)", fontsize=12)
    plt.ylabel("Salary ($M)", fontsize=12)
    # 5. Save Output
    os.makedirs("plots", exist_ok=True)
    output_path = "plots/war_vs_salary.png"
    plt.savefig(output_path)
    print(f"✅ Plot saved to {output_path}")


if __name__ == "__main__":
    plot_war_vs_salary(2024)
