type VolvoLogoProps = {
  className?: string;
};

export default function VolvoLogo({ className }: VolvoLogoProps) {
  return (
    <svg
      viewBox="0 0 128 128"
      className={className}
      aria-hidden="true"
      focusable="false"
    >
      <circle
        cx="52"
        cy="70"
        r="34"
        fill="none"
        stroke="currentColor"
        strokeWidth="5.5"
      />
      <line
        x1="68"
        y1="34"
        x2="82"
        y2="46"
        stroke="currentColor"
        strokeWidth="5"
        strokeLinecap="round"
      />
      <polygon points="81,30 96,29 92,43" fill="currentColor" />
      <text
        x="52"
        y="73"
        textAnchor="middle"
        fill="currentColor"
        fontFamily="Georgia, 'Times New Roman', serif"
        fontSize="16"
        fontWeight="700"
        letterSpacing="1.2"
      >
        VOLVO
      </text>
    </svg>
  );
}
