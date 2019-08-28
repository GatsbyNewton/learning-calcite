package edu.wzm.sample;

import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
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


public class HepPlannerTest {
    private static final Logger LOGGER = LogManager.getLogger(HepPlannerTest.class);;

    public static HepPlanner getHepPlanner(){
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FILTER_ON_JOIN);
        builder.addRuleInstance(ReduceExpressionsRule.PROJECT_INSTANCE);
        builder.addRuleInstance(PruneEmptyRules.PROJECT_INSTANCE);

        return new HepPlanner(builder.build());
    }

    public static void main(String... args) throws Exception{
        String sql = "select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age \n" +
                "from users u join jobs j on u.name=j.name\n" +
                "where u.age > 30 and j.id>10\n" +
                "order by user_id";

        SchemaPlus rootSchema = CalciteUtil.getSchemaPlus();

        final FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .defaultSchema(rootSchema)
                .parserConfig(SqlParser.Config.DEFAULT)
                .traitDefs(ConventionTraitDef.INSTANCE, RelDistributionTraitDef.INSTANCE)
                .build();

        /** use HepPlanner */
        HepPlanner planner = getHepPlanner();

        SqlTypeFactoryImpl factory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        /** SQL parser */
        SqlParser parser = SqlParser.create(sql, SqlParser.Config.DEFAULT);
        SqlNode parsedSqlNode = parser.parseStmt();
        LOGGER.info("The SqlNode after parsed: \n{}\n", parsedSqlNode.toString());

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema.from(rootSchema),
                CalciteSchema.from(rootSchema).path(null),
                factory,
                new CalciteConnectionConfigImpl(new Properties()));

        /** SQL validate */
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

        planner.setRoot(relNode);
        relNode = planner.findBestExp();
        LOGGER.info("The BEST Relational Expression string: \n{}\n",
                RelOptUtil.toString(relNode, SqlExplainLevel.ALL_ATTRIBUTES));
    }
}
