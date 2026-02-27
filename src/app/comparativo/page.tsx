import { getYearComparison, getCachedAllYearsTrend, getCachedAvailableYears, type YearComparisonData } from "@/data/get-members";
import { ComparativoClient } from "./comparativo-client";

interface Props {
  searchParams: Promise<{ ano1?: string; ano2?: string }>;
}

function isValidYear(ano: string): boolean {
  return /^\d{4}$/.test(ano);
}

export default async function ComparativoPage({ searchParams }: Props) {
  const { ano1, ano2 } = await searchParams;
  const availableYears = await getCachedAvailableYears();

  const years = availableYears.map((y) => parseInt(y.value));
  const defaultYear1 = years.length >= 2 ? years[1] : years[0];
  const defaultYear2 = years[0];

  const year1 = ano1 && isValidYear(ano1) ? parseInt(ano1) : defaultYear1;
  const year2 = ano2 && isValidYear(ano2) ? parseInt(ano2) : defaultYear2;

  const comparison = getYearComparison(year1, year2);
  const trend = await getCachedAllYearsTrend();

  return (
    <ComparativoClient
      year1={year1}
      year2={year2}
      availableYears={availableYears}
      comparison={comparison}
      trend={trend}
    />
  );
}
