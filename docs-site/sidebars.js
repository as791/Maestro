/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'intro',
    'getting-started',
    'comparison',
    {
      type: 'category',
      label: 'SDKs',
      items: ['sdk/python', 'sdk/go', 'sdk/java'],
    },
    {
      type: 'category',
      label: 'Autoscaling',
      items: ['autoscaling/overview', 'autoscaling/kafka-lag', 'autoscaling/cpu-based', 'autoscaling/lambda-deployment'],
    },
    'api-reference',
    'architecture',
    'eks-deployment',
    'safety',
    'configuration',
  ],
};

export default sidebars;
