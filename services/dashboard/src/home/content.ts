// Public awareness content for the FORESENTRY home page. Written for people paying
// and getting paid in India. Kept in one place so it can be reviewed and translated.

export const HELPLINE = "1930";
export const REPORT_URL = "https://cybercrime.gov.in";

export interface Article {
  slug: string;
  title: string;
  summary: string;
  tag: string;
  minutes: number;
  /** Paragraphs; a paragraph starting with "- " is rendered as a list item. */
  body: string[];
}

export const ARTICLES: Article[] = [
  {
    slug: "collect-request",
    title: "You never enter a PIN to receive money",
    summary: "The UPI collect-request trick: a 'payment' that actually asks you to pay.",
    tag: "UPI",
    minutes: 3,
    body: [
      "A buyer on a marketplace says they have sent you money and asks you to 'accept' it in your UPI app. What arrives is a collect request: a request for you to pay them. Entering your UPI PIN approves it, and the money leaves your account.",
      "The rule that stops this every time: receiving money never needs your UPI PIN. The PIN is only ever for money going out.",
      "Warning signs:",
      "- The sender pushes you to act quickly, or calls while you are on the screen.",
      "- The screen says 'Pay' or shows a debit, even though you were told you are receiving.",
      "- You are asked to scan a QR code 'to receive' money. Scanning a QR code is also for paying.",
      "If you approved one by mistake, call your bank to report it, then call 1930 or file at cybercrime.gov.in. Speed matters: the sooner it is reported, the better the chance the money can be held.",
    ],
  },
  {
    slug: "kyc-call",
    title: "'Your account will be blocked today': fake KYC calls",
    summary: "Calls and messages that threaten a block unless you update KYC through a link.",
    tag: "Impersonation",
    minutes: 3,
    body: [
      "A message or call claims to be from your bank, wallet or telecom provider. It says your KYC has expired and the account will be blocked within hours unless you update it now, using a link or an app they name.",
      "The link leads to a copy of a real login page, or the app gives the caller control of your phone. Either way, they collect your login, card details or OTP and empty the account.",
      "What real providers do and do not do:",
      "- They do not ask you to install an app to 'update KYC' on a phone call.",
      "- They do not ask for your OTP, card PIN, CVV or UPI PIN, for any reason.",
      "- A deadline of a few hours, with a threat, is a pressure tactic.",
      "To check, hang up and call the number printed on your card or on the provider's official website. Do not use a number from the message.",
    ],
  },
  {
    slug: "screen-share",
    title: "Remote-access apps and screen sharing",
    summary: "Why a 'support agent' wants you to install an app, and what it lets them see.",
    tag: "Device",
    minutes: 2,
    body: [
      "Someone offering to fix a refund, a failed payment or a blocked card asks you to install a screen-sharing or remote-support app and read out a code.",
      "Once connected, they can watch every OTP and PIN you type, and on some phones they can tap for you.",
      "- No bank or payment app needs to see your screen to help you.",
      "- If you have installed such an app at someone's request, uninstall it, restart the phone and change your banking passwords from a different device.",
      "- Tell your bank so it can watch the account.",
    ],
  },
  {
    slug: "task-scam",
    title: "Paid to like videos: the task and job scam",
    summary: "Small early payouts, then a 'deposit' to unlock bigger tasks that never pays back.",
    tag: "Investment",
    minutes: 3,
    body: [
      "A message offers easy part-time work: like videos, review hotels, rate products. The first few tasks pay a small amount, which makes it feel real.",
      "Then you are moved to 'prepaid' or 'premium' tasks that need a deposit, with a promise of a larger return. The balance on their website grows, but withdrawals are blocked until you pay a 'tax', a 'fee' or one more task.",
      "- Real employers do not ask you to pay to work.",
      "- A dashboard showing your earnings is just a web page; the numbers on it can be anything.",
      "- Stop paying the moment a withdrawal needs another payment. Report it at cybercrime.gov.in with the chat history and every transaction reference.",
    ],
  },
  {
    slug: "digital-arrest",
    title: "No police officer arrests you on a video call",
    summary: "Callers posing as police, customs or courts who demand payment to 'clear your name'.",
    tag: "Impersonation",
    minutes: 3,
    body: [
      "A caller says a parcel in your name contained illegal items, or your ID was used in a crime. You are passed to an 'officer' on a video call, sometimes in uniform, and told you are under investigation and must not hang up or tell anyone.",
      "They then ask you to move money to a 'safe' or 'verification' account until the case is cleared.",
      "- Police and courts do not hold you on a video call or ask you to transfer money to prove anything.",
      "- Being told to keep it secret from your family is a sign of a scam, not of an investigation.",
      "- End the call. Talk to someone you trust. Report it on 1930.",
    ],
  },
  {
    slug: "after-fraud",
    title: "Lost money to fraud? Do these things first",
    summary: "The order of steps that gives you the best chance to stop the money moving on.",
    tag: "Help",
    minutes: 2,
    body: [
      "Fraudsters move money through several accounts quickly. Reporting early lets banks try to hold it before it is withdrawn.",
      "- Call 1930, the national cyber-crime helpline, and give the transaction references.",
      "- Call your bank's official number to block the card, UPI or net banking that was used.",
      "- File a complaint at cybercrime.gov.in. Keep the acknowledgement number.",
      "- Keep screenshots of messages, call logs, links and every transaction ID. Do not delete the chat.",
      "- Change passwords for email and banking from a device you trust.",
      "Be careful of anyone who contacts you offering to recover the money for a fee. That is often a second scam.",
    ],
  },
];

export interface QuizItem { scenario: string; scam: boolean; why: string }

export const QUIZ: QuizItem[] = [
  { scenario: "A buyer sends you a UPI request and says 'enter your PIN to receive ₹4,500 for the sofa'.", scam: true,
    why: "Receiving money never needs your UPI PIN. This is a collect request: entering the PIN pays them." },
  { scenario: "Your bank's app shows a notification for a payment you just made at a shop.", scam: false,
    why: "A notification inside the official app for a payment you made is normal. Nobody is asking you to do anything." },
  { scenario: "A caller from 'customer care' asks you to install a support app so they can fix a failed refund.", scam: true,
    why: "Remote-access apps let the caller see your OTPs and PINs. Real support never needs your screen." },
  { scenario: "A website pays you ₹150 for liking three videos, then asks for a ₹2,000 deposit to unlock 'VIP tasks'.", scam: true,
    why: "Small payouts build trust; the deposit is the scam. Employers do not charge you to work." },
  { scenario: "You get an OTP by SMS right after you started a payment yourself on a site you trust.", scam: false,
    why: "An OTP you asked for, entered on the page you are using, is how it should work. Never read it out to a caller." },
];

export const BANNER_TIPS = [
  "Never share your OTP, UPI PIN or card PIN with anyone, including people who say they are from your bank.",
  "Receiving money never needs your UPI PIN. If an app asks for it, you are paying, not receiving.",
  "Lost money to fraud? Call 1930 or report at cybercrime.gov.in as soon as you can.",
];
