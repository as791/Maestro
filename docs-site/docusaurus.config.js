import { themes as prismThemes } from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Cohestra — Control Plane',
  tagline: 'Open-source deployment management for Apache Flink on any Kubernetes cluster',
  favicon: 'img/favicon.png',

  future: { v4: true },

  url: 'https://cohestra.dev',
  baseUrl: '/',

  organizationName: 'as791',
  projectName: 'Cohestra',

  onBrokenLinks: 'throw',

  i18n: { defaultLocale: 'en', locales: ['en'] },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/as791/Cohestra/tree/main/docs-site/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      image: 'img/cohestra-social-card.png',
      colorMode: {
        defaultMode: 'light',
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'Cohestra',
        logo: {
          alt: 'Cohestra Logo',
          src: 'img/logo.png',
          width: 56,
          height: 56,
        },
        items: [
          { type: 'docSidebar', sidebarId: 'docsSidebar', position: 'left', label: 'Docs' },
          { to: '/docs/sdk/python', label: 'Python SDK', position: 'left' },
          { to: '/docs/sdk/go', label: 'Go SDK', position: 'left' },
          { to: '/docs/sdk/java', label: 'Java SDK', position: 'left' },
          {
            href: 'https://github.com/as791/Cohestra',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'light',
        logo: {
          alt: 'Cohestra Logo',
          src: 'img/logo.png',
          width: 48,
          height: 48,
        },
        links: [
          {
            title: 'Documentation',
            items: [
              { label: 'Getting Started', to: '/docs/getting-started' },
              { label: 'Comparison', to: '/docs/comparison' },
              { label: 'Autoscaling', to: '/docs/autoscaling/overview' },
            ],
          },
          {
            title: 'SDKs',
            items: [
              { label: 'Python', to: '/docs/sdk/python' },
              { label: 'Go', to: '/docs/sdk/go' },
              { label: 'Java', to: '/docs/sdk/java' },
            ],
          },
          {
            title: 'Community',
            items: [
              { label: 'GitHub', href: 'https://github.com/as791/Cohestra' },
              { label: 'Contributing', href: 'https://github.com/as791/Cohestra/blob/main/CONTRIBUTING.md' },
            ],
          },
        ],
        copyright: `Copyright ${new Date().getFullYear()} Cohestra Contributors. Apache-2.0 Licensed.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['java', 'bash', 'json', 'yaml', 'go'],
      },
    }),
};

export default config;
