import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { StackEmptyHint } from "@/components/layout/StackEmptyHint";

type PlaceholderPageProps = {
  title: string;
  description: string;
};

export function PlaceholderPage({ title, description }: PlaceholderPageProps) {
  return (
    <Card className="max-w-2xl border-dashed">
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4 text-sm text-muted-foreground">
        <p>This section will be wired to backend APIs in a later milestone.</p>
        <StackEmptyHint />
      </CardContent>
    </Card>
  );
}
