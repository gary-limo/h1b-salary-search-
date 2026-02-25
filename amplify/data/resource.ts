import { type ClientSchema, a, defineData } from "@aws-amplify/backend";

/*== H1B Data Model =========================================================
Schema for H1B visa petition data. All fields are optional to support
partial records and flexible data import.

Primary index: default id (partition key). Amplify auto-generates this.

Secondary indexes (GSIs):
  Individual: employerName, jobTitle, worksiteCity
  Combinations: employerName+jobTitle, employerName+worksiteCity, jobTitle+worksiteCity
  (DynamoDB supports 1 partition + 1 sort key per index; 3-way combo requires app filter)
=========================================================================*/
const schema = a.schema({
  H1B: a
    .model({
      caseNumber: a.string(),
      status: a.string(),
      receivedDate: a.date(),
      decisionDate: a.date(),
      visaClass: a.string(),
      jobTitle: a.string(),
      socCode: a.string(),
      socTitle: a.string(),
      fullTimePosition: a.string(),
      beginDate: a.date(),
      endDate: a.date(),
      totalWorkerPositions: a.integer(),
      newEmployment: a.string(),
      continuedEmployment: a.string(),
      changePreviousEmployment: a.string(),
      newConcurrentEmployment: a.string(),
      changeEmployer: a.string(),
      amendedPetition: a.string(),
      employerName: a.string(),
      employerAddress1: a.string(),
      employerCity: a.string(),
      employerState: a.string(),
      employerPostalCode: a.string(),
      employerCountry: a.string(),
      employerPhone: a.string(),
      employerFein: a.string(),
      naicsCode: a.string(),
      employerPocLastName: a.string(),
      employerPocFirstName: a.string(),
      employerPocJobTitle: a.string(),
      employerPocAddress1: a.string(),
      employerPocCity: a.string(),
      employerPocState: a.string(),
      employerPocPostalCode: a.string(),
      employerPocCountry: a.string(),
      employerPocPhone: a.string(),
      employerPocEmail: a.string(),
      agentRepresentingEmployer: a.string(),
      worksiteWorkers: a.string(),
      secondaryEntity: a.string(),
      secondaryEntityBusinessName: a.string(),
      worksiteAddress1: a.string(),
      worksiteCity: a.string(),
      worksiteCounty: a.string(),
      worksiteState: a.string(),
      worksitePostalCode: a.string(),
      wageRateOfPayFrom: a.string(),
      wageRateOfPayTo: a.string(),
      wageUnitOfPay: a.string(),
      prevailingWage: a.string(),
      pwUnitOfPay: a.string(),
      pwWageLevel: a.string(),
      pwOesYear: a.string(),
      totalWorksiteLocations: a.integer(),
      agreeToLcStatement: a.string(),
      h1bDependent: a.string(),
      willfulViolator: a.string(),
      publicDisclosure: a.string(),
    })
    .secondaryIndexes((index) => [
      index("employerName"),
      index("jobTitle"),
      index("worksiteCity"),
      index("employerName").sortKeys(["jobTitle"]),
      index("employerName").sortKeys(["worksiteCity"]),
      index("jobTitle").sortKeys(["worksiteCity"]),
    ])
    .authorization((allow) => [allow.publicApiKey().to(["read"])]),
});

export type Schema = ClientSchema<typeof schema>;

export const data = defineData({
  schema,
  authorizationModes: {
    defaultAuthorizationMode: "apiKey",
    apiKeyAuthorizationMode: {
      expiresInDays: 365,
    },
  },
});

