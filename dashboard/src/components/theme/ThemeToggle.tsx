import { Monitor, Moon, Sun } from "lucide-react";
import { useTheme } from "next-themes";
import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export function ThemeToggle() {
  const { theme, setTheme } = useTheme();
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  if (!mounted) {
    return <div className="h-9 w-[108px] rounded-md border border-border bg-muted/30" aria-hidden />;
  }

  const Item = ({
    value,
    icon: Icon,
    label,
  }: {
    value: "light" | "dark" | "system";
    icon: typeof Sun;
    label: string;
  }) => (
    <Button
      type="button"
      variant={theme === value ? "secondary" : "ghost"}
      size="sm"
      className={cn("h-8 px-2", theme === value && "shadow-sm")}
      onClick={() => setTheme(value)}
      aria-label={label}
      title={label}
    >
      <Icon className="h-4 w-4" />
    </Button>
  );

  return (
    <div
      className="inline-flex rounded-md border border-border bg-card p-0.5 shadow-sm"
      role="group"
      aria-label="Theme"
    >
      <Item value="light" icon={Sun} label="Light theme" />
      <Item value="dark" icon={Moon} label="Dark theme" />
      <Item value="system" icon={Monitor} label="System theme" />
    </div>
  );
}
