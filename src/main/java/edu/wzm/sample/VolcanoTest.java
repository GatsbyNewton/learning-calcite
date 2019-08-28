package edu.wzm.sample;


import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class VolcanoTest {
    private static final Logger LOGGER = LogManager.getLogger(VolcanoTest.class);

    private static VolcanoPlanner getVolcanoPlanner(){
        VolcanoPlanner planner = new VolcanoPlanner();

        /** add TraitDef */
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);

        /** add rule */
        planner.addRule(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
        planner.addRule(ReduceExpressionsRule.PROJECT_INSTANCE);
        planner.addRule(PruneEmptyRules.PROJECT_INSTANCE);

        /** add ConverterRule */
        planner.addRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.addRule(EnumerableRules.ENUMERABLE_FILTER_RULE);

        return planner;
    }

    public static void main(String... args)throws Exception{
//        String sql = "select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age \n" +
//                "from users u join jobs j on u.name=j.name\n" +
//                "where u.age > 30 and j.id>10\n" +
//                "order by user_id";
        String sql = "select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age \n"
                + "from users u join jobs j on u.id = j.id \n"
                + "where u.age > 30 and j.id > 10 \n"
                + "order by user_id";

        SchemaPlus rootSchema = CalciteUtil.getSchemaPlus();
        final FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema)
                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                .build();

        VolcanoPlanner planner = getVolcanoPlanner();

        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        /** SQL parser */
        SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
        SqlNode parsedSqlNode = parser.parseStmt();
        LOGGER.info("The SqlNode after parsed: \n{}\n", parsedSqlNode.toString());

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                CalciteSchema.from(rootSchema),
                CalciteSchema.from(rootSchema).path(null),
                factory,
                new CalciteConnectionConfigImpl(new Properties()));

        /** Sql validate */
        SqlValidator validator = SqlValidatorUtil.newValidator(SqlStdOperatorTable.instance(),
                catalogReader,
                factory,
                CalciteUtil.conformance(frameworkConfig));
        SqlNode validatedSqlNode = validator.validate(parsedSqlNode);
        LOGGER.info("The SqlNode after validated: \n{}\n", validatedSqlNode.toString());

        final RexBuilder rexBuilder = CalciteUtil.createRexBuilder(factory);
        final RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        /** init SqlToRelConverter config */
        final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                .withConfig(frameworkConfig.getSqlToRelConverterConfig())
                .withTrimUnusedFields(false)
                .withConvertTableAccess(false)
                .build();

        /** SqlNode to RelNode */
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(new CalciteUtil.ViewExpanderImpl(),
                validator,
                catalogReader,
                cluster,
                frameworkConfig.getConvertletTable(),
                config);
        RelRoot root = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);

        root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        final RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null);
        root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
        RelNode relNode = root.rel;
        LOGGER.info("The Relational Expression string before optimized: \n{}\n",
                RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES));

        RelTraitSet desiredTraits = relNode.getCluster()
                .traitSet()
                .replace(EnumerableConvention.INSTANCE);
        relNode = planner.changeTraits(relNode, desiredTraits);

        planner.setRoot(relNode);
        relNode = planner.findBestExp();
        LOGGER.info("The BEST Relational Expression string: \n{}\n",
                RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES));
    }
}
