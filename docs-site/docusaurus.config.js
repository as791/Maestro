import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Maestro — Flink Control Plane',
  tagline: 'Open-source deployment management for Apache Flink on any Kubernetes cluster',
  favicon: 'img/favicon.png',

  future: { v4: true },

  url: 'https://maestro.dev',
  baseUrl: '/',

  organizationName: 'maestro-flink',
  projectName: 'maestro',

  onBrokenLinks: 'throw',

  i18n: { defaultLocale: 'en', locales: ['en'] },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/maestro-flink/maestro/tree/main/docs-site/',
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
      image: 'img/maestro-social-card.png',
      colorMode: {
        defaultMode: 'light',
        respectPrefersColorScheme: true,
      },
      navbar: {
        title: 'Maestro',
        logo: {
          alt: 'Maestro Logo',
          src: 'img/logo.png',
        },
        items: [
          { type: 'docSidebar', sidebarId: 'docsSidebar', position: 'left', label: 'Docs' },
          { to: '/docs/sdk/python', label: 'Python SDK', position: 'left' },
          { to: '/docs/sdk/go', label: 'Go SDK', position: 'left' },
          { to: '/docs/sdk/java', label: 'Java SDK', position: 'left' },
          { to: '/docs/api-reference', label: 'API', position: 'left' },
          {
            href: 'https://github.com/maestro-flink/maestro',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'light',
        links: [
          {
            title: 'Documentation',
            items: [
              { label: 'Getting Started', to: '/docs/getting-started' },
              { label: 'API Reference', to: '/docs/api-reference' },
              { label: 'Architecture', to: '/docs/architecture' },
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
              { label: 'GitHub', href: 'https://github.com/maestro-flink/maestro' },
              { label: 'Contributing', href: 'https://github.com/maestro-flink/maestro/blob/main/CONTRIBUTING.md' },
            ],
          },
        ],
        copyright: `Copyright ${new Date().getFullYear()} Maestro Contributors. Apache-2.0 Licensed.`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['java', 'bash', 'json', 'yaml', 'go'],
      },
    }),
};

export default config;
