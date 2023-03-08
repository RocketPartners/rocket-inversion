package io.rcktapp.api;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

class ApiTest
{
    @Test
    void getEndpoints()
    {
        Api underTest = new Api();
        Endpoint early = new Endpoint();
        early.setOrder(1);
        early.setName("early");
        Endpoint normal = new Endpoint();
        normal.setName("normal");
        Endpoint similar = new Endpoint();
        similar.setName("similar");
        Endpoint late = new Endpoint();
        late.setName("late");
        late.setOrder(1100);
        underTest.addEndpoint(late);
        underTest.addEndpoint(normal);
        underTest.addEndpoint(similar);
        underTest.addEndpoint(early);
        assertThat(underTest.getEndpoints(), containsInAnyOrder(early, normal, late, similar));
        assertThat(underTest.getEndpoints().indexOf(early), equalTo(0));
        assertThat(underTest.getEndpoints().indexOf(normal), equalTo(1));
        assertThat(underTest.getEndpoints().indexOf(similar), equalTo(2));
        assertThat(underTest.getEndpoints().indexOf(late), equalTo(3));
    }
}
