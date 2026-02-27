import { getAnomalias, getCachedAvailableYears } from "@/data/get-members";
import { AnomaliasClient } from "./anomalias-client";

interface Props {
  searchParams: Promise<{ ano?: string; min?: string }>;
}

function isValidYear(ano: string): boolean {
  return /^\d{4}$/.test(ano);
}

export default async function AnomaliasPage({ searchParams }: Props) {
  const { ano, min } = await searchParams;
  const availableYears = await getCachedAvailableYears();
  const validAno = ano && isValidYear(ano) ? ano : undefined;
  const currentYear =
    validAno ||
    availableYears.find((y) => y.value === "2025")?.value ||
    (availableYears.length > 0 ? availableYears[0].value : "2025");

  const minVariacao = min ? parseInt(min) : 200;
  const anomalias = getAnomalias(parseInt(currentYear), minVariacao);

  return (
    <AnomaliasClient
      anomalias={anomalias}
      availableYears={availableYears}
      currentYear={currentYear}
      minVariacao={minVariacao}
    />
  );
}
