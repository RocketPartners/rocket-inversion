package io.rcktapp.api;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.containsInAnyOrder;

class ApiTest
{
    @Test
    void getEndpoints()
    {
        Api underTest = new Api();
        Endpoint early = new Endpoint();
        early.setOrder(1);
        Endpoint normal = new Endpoint();
        Endpoint late = new Endpoint();
        late.setOrder(1100);
        underTest.addEndpoint(late);
        underTest.addEndpoint(normal);
        underTest.addEndpoint(early);
        assertThat(underTest.getEndpoints(), containsInAnyOrder(early, normal, late));
        assertThat(underTest.getEndpoints().indexOf(early), equalTo(0));
        assertThat(underTest.getEndpoints().indexOf(normal), equalTo(1));
        assertThat(underTest.getEndpoints().indexOf(late), equalTo(2));
    }
}
