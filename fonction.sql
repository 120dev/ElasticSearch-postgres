CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$
    DECLARE
        data json;
        notification json;
        id integer;
    BEGIN
        -- Convert the old or new row to JSON, based on the kind of action.
        -- Action = DELETE?             -> OLD row
        -- Action = INSERT or UPDATE?   -> NEW row
        IF (TG_OP = 'DELETE') THEN
            data = row_to_json(OLD);
            id = OLD.id;
        ELSE
            data = row_to_json(NEW);
            id = NEW.id;
        END IF;
        -- Contruct the notification as a JSON string.
        notification = json_build_object(
                          'table',TG_TABLE_NAME,
                          'action', TG_OP,
                          'id', id,
                          'data', data);
        -- Execute pg_notify(channel, notification)
        PERFORM pg_notify('events',notification::text);
        -- Result is ignored since this is an AFTER trigger
        RETURN NULL;
    END;
$$ LANGUAGE plpgsql;



CREATE TRIGGER products_notify_event
AFTER INSERT OR UPDATE OR DELETE 
ON public.users
FOR EACH ROW 
EXECUTE PROCEDURE public.notify_event();





INSERT INTO public.users(id, role, state, email, password, civility, lastname, firstname, address, street, district, city, zip_code, post_box, phone, mobile, fax, last_env, reset_password_token, reset_password_sent_at, sign_in_count, current_sign_in_at, last_sign_in_at, current_sign_in_ip, last_sign_in_ip, created_at, updated_at)
VALUES (50, 'user', 'active', 'jeje@jeje.nc', 'test', null, 'moi', 'moi', null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);

UPDATE public.users SET email='jcante@u2nc.nc' WHERE id=50;

DELETE FROM public.users WHERE id = 50;
