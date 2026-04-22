type Props = {
  message?: string | null;
};

export default function ReadingStatusBanner({ message }: Props) {
  if (!message) return null;
  return (
    <div className="readingStatusBanner" role="status" aria-live="polite">
      {message}
    </div>
  );
}
