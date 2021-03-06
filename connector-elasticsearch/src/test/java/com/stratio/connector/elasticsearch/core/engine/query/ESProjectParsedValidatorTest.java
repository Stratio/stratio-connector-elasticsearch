package com.stratio.connector.elasticsearch.core.engine.query;

import com.stratio.connector.commons.engine.query.ProjectParsed;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Window;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * ESProjectParsedValidator Tester.
 * 
 * @author <Authors name>
 * @version 1.0
 */
public class ESProjectParsedValidatorTest {

    private static final ClusterName CLUSTER_NAME = new ClusterName("clusterName");
    private static final TableName TABLE_NAME = new TableName("catalog_name", "table_name");

    /**
     * 
     * Method: validate(ProjectParsed projectParsed)
     * 
     */
    @Test(expected = UnsupportedException.class)
    public void testValidate() throws Exception {

        ESProjectParsedValidator esProjectParsedValidator = new ESProjectParsedValidator();

        ProjectParsed projectParsed = mock(ProjectParsed.class);
        Window window = mock(Window.class);
        when(projectParsed.getWindow()).thenReturn(window);

        esProjectParsedValidator.validate(projectParsed);
    }

}
