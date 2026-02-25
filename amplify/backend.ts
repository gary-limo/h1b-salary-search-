import { defineBackend } from '@aws-amplify/backend';
import { data } from './data/resource.js';

// Trigger backend redeploy to refresh AppSync API key (fixes 401)
import * as wafv2 from 'aws-cdk-lib/aws-wafv2';
import * as appsync from 'aws-cdk-lib/aws-appsync';

const backend = defineBackend({
  data,
});

// Allowed origins: set ALLOWED_ORIGINS env var (comma-separated domains).
// Defaults to localhost for dev. Example:
//   ALLOWED_ORIGINS=myapp.amplifyapp.com,mycustomdomain.com npx ampx sandbox
//
// Matching uses "://domain" prefix to prevent substring attacks
// (e.g. "localhost" won't match "maliciouslocalhost.com")
const rawOrigins = (process.env.ALLOWED_ORIGINS ?? 'localhost')
  .split(',')
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

const allowedPatterns = rawOrigins.flatMap((domain) => [
  `://${domain}`,
  `://www.${domain}`,
]);

// Allowed countries: USA, Singapore, India, Canada. Set GEO_ALLOWED_COUNTRIES to override.
const allowedCountriesRaw = (process.env.GEO_ALLOWED_COUNTRIES ?? 'US,SG,IN,CA')
  .split(',')
  .map((s) => s.trim().toUpperCase())
  .filter((s) => s.length === 2);
const allowedCountries = allowedCountriesRaw.length > 0 ? allowedCountriesRaw : ['US', 'SG', 'IN', 'CA'];

// Enable AppSync response caching (reduces DynamoDB reads)
const graphqlApi = backend.data.resources.graphqlApi;
new appsync.CfnApiCache(graphqlApi.stack, 'AppSyncApiCache', {
  apiId: graphqlApi.apiId,
  apiCachingBehavior: 'FULL_REQUEST_CACHING',
  type: 'SMALL',
  ttl: 3600,
  healthMetricsConfig: 'ENABLED',
});

// Create a custom stack for WAF (same region as AppSync)
const wafStack = backend.createStack('AppSyncWAF');

// Build origin-check statements: allow if referer OR origin contains "://domain"
const originStatements: wafv2.CfnWebACL.StatementProperty[] = allowedPatterns.flatMap(
  (pattern) => [
    {
      byteMatchStatement: {
        fieldToMatch: { singleHeader: { name: 'referer' } },
        positionalConstraint: 'CONTAINS',
        searchString: pattern,
        textTransformations: [{ priority: 0, type: 'LOWERCASE' }],
      },
    },
    {
      byteMatchStatement: {
        fieldToMatch: { singleHeader: { name: 'origin' } },
        positionalConstraint: 'CONTAINS',
        searchString: pattern,
        textTransformations: [{ priority: 0, type: 'LOWERCASE' }],
      },
    },
  ]
);

const webAcl = new wafv2.CfnWebACL(wafStack, 'AppSyncWebACL', {
  scope: 'REGIONAL',
  defaultAction: { allow: {} },
  rules: [
    {
      name: 'allow-only-trusted-origins',
      priority: 1,
      statement: {
        notStatement: {
          statement:
            originStatements.length === 1
              ? originStatements[0]
              : { orStatement: { statements: originStatements } },
        },
      },
      action: { block: {} },
      visibilityConfig: {
        sampledRequestsEnabled: true,
        cloudWatchMetricsEnabled: true,
        metricName: 'allow-only-trusted-origins',
      },
    },
    {
      name: 'geo-restrict',
      priority: 2,
      statement: {
        notStatement: {
          statement: {
            geoMatchStatement: {
              countryCodes: allowedCountries,
            },
          },
        },
      },
      action: { block: {} },
      visibilityConfig: {
        sampledRequestsEnabled: true,
        cloudWatchMetricsEnabled: true,
        metricName: 'geo-restrict',
      },
    },
    {
      name: 'rate-limit-api',
      priority: 4,
      statement: {
        rateBasedStatement: {
          limit: 500,
          aggregateKeyType: 'IP',
        },
      },
      action: { block: {} },
      visibilityConfig: {
        sampledRequestsEnabled: true,
        cloudWatchMetricsEnabled: true,
        metricName: 'rate-limit-api',
      },
    },
  ],
  visibilityConfig: {
    sampledRequestsEnabled: true,
    cloudWatchMetricsEnabled: true,
    metricName: 'AppSyncWebACL',
  },
});

// Associate the Web ACL with the AppSync API
new wafv2.CfnWebACLAssociation(wafStack, 'AppSyncWAFAssociation', {
  resourceArn: backend.data.resources.graphqlApi.arn,
  webAclArn: webAcl.attrArn,
});