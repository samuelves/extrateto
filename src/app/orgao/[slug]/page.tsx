import { getMembersByYear, getCachedAvailableYears } from "@/data/get-members";
import { OrgaoDetailClient } from "./orgao-detail-client";

interface Props {
  searchParams: Promise<{ ano?: string }>;
}

function isValidYear(ano: string): boolean {
  return /^\d{4}$/.test(ano);
}

export default async function OrgaoPage({ searchParams }: Props) {
  const { ano } = await searchParams;
  const availableYears = await getCachedAvailableYears();
  const validAno = ano && isValidYear(ano) ? ano : undefined;
  const currentYear =
    validAno ||
    availableYears.find((y) => y.value === "2025")?.value ||
    (availableYears.length > 0 ? availableYears[0].value : "2025");
  const members = getMembersByYear(currentYear);
  return (
    <OrgaoDetailClient
      members={members}
      availableYears={availableYears}
      currentYear={currentYear}
    />
  );
}
