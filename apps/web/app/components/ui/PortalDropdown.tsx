"use client";

import { ReactNode, RefObject, useEffect, useState } from "react";
import { createPortal } from "react-dom";

interface PortalDropdownProps {
  open: boolean;
  onClose: () => void;
  /** Ref do elemento âncora (o botão que abre o menu). */
  anchorRef: RefObject<HTMLElement | null>;
  children: ReactNode;
  /** Largura mínima do menu (default: largura da âncora). */
  minWidth?: number;
}

/**
 * Menu suspenso renderizado via portal no ``document.body`` com ``position:fixed``
 * e z-index altíssimo. Isso faz o menu escapar de QUALQUER contexto de
 * empilhamento (cards com ``backdrop-filter``, ``transform`` etc.) e ficar SEMPRE
 * à frente, clicável. Reposiciona no scroll/resize e fecha em clique-fora/Esc.
 *
 * Uso: passe a ref do botão em ``anchorRef`` e o conteúdo (a lista) como filho.
 */
export default function PortalDropdown({
  open,
  onClose,
  anchorRef,
  children,
  minWidth,
}: PortalDropdownProps) {
  const [mounted, setMounted] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number; width: number; maxH: number } | null>(null);

  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    if (!open) return;

    const reposition = () => {
      const el = anchorRef.current;
      if (!el) return;
      const r = el.getBoundingClientRect();
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      const width = Math.max(minWidth ?? 0, r.width);
      let left = r.left;
      if (left + width > vw - 8) left = Math.max(8, vw - width - 8);
      if (left < 8) left = 8;
      const top = r.bottom + 6;
      const maxH = Math.max(120, vh - top - 12);
      setPos({ top, left, width, maxH });
    };

    reposition();

    const onScrollResize = () => reposition();
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (anchorRef.current?.contains(t)) return; // clique no botão: deixa o toggle agir
      const menu = document.getElementById("__portal_dropdown_open");
      if (menu?.contains(t)) return; // clique dentro do menu
      onClose();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };

    window.addEventListener("scroll", onScrollResize, true);
    window.addEventListener("resize", onScrollResize);
    document.addEventListener("mousedown", onDown, true);
    document.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("scroll", onScrollResize, true);
      window.removeEventListener("resize", onScrollResize);
      document.removeEventListener("mousedown", onDown, true);
      document.removeEventListener("keydown", onKey);
    };
  }, [open, anchorRef, minWidth, onClose]);

  if (!mounted || !open || !pos) return null;

  return createPortal(
    <div
      id="__portal_dropdown_open"
      style={{
        position: "fixed",
        top: pos.top,
        left: pos.left,
        minWidth: pos.width,
        maxHeight: pos.maxH,
        overflowY: "auto",
        zIndex: 100000,
      }}
    >
      {children}
    </div>,
    document.body,
  );
}
